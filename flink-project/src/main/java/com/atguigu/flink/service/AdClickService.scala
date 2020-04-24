package com.atguigu.flink.service

import java.lang

import com.atguigu.flink.bean.{AdClickLog, CountByProvince}
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.AdClickDao
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdClickService  extends   TService{
  private val adClickDao = new AdClickDao
  override def getDao(): TDao = adClickDao

  override def analyses() = {


    val fileDS: DataStream[String] = adClickDao.readTextFile("input/AdClickLog.csv")


    val dataDS: DataStream[AdClickLog] = fileDS.map(data => {
      val datas: Array[String] = data.split(",")
      AdClickLog(
        datas(0).toLong,
        datas(1).toLong,
        datas(2),
        datas(3),
        datas(4).toLong
      )
    })

    val timeDS: DataStream[AdClickLog] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)
    val mapDS: DataStream[(String, Long)] = timeDS.map(d => {
      ((d.province + "-" + d.adId), 1L)
    })

    val dataKS: KeyedStream[(String, Long), String] = mapDS.keyBy(_._1)

    val dataWS: WindowedStream[(String, Long), String, TimeWindow] = dataKS.timeWindow(Time.hours(1), Time.seconds(5))


    val aggDS: DataStream[CountByProvince] = dataWS.aggregate(
      new AggregateFunction[(String, Long), Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1L

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
      }
      ,
      new WindowFunction[Long, CountByProvince, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
          val ks = key.split("_")
          val count = input.iterator.next
          out.collect(
            CountByProvince(
              window.getEnd.toString,
              ks(0),
              ks(1).toLong,
              count
            )
          )
        }
      }
    )
    val datakST: KeyedStream[CountByProvince, String] = aggDS.keyBy(_.windowEnd)
    datakST.process(
      new KeyedProcessFunction[String, CountByProvince, String] {
        override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[String, CountByProvince, String]#Context, out: Collector[String]): Unit = {

        }
      }
    )







  }
}
