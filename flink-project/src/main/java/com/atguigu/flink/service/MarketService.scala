package com.atguigu.flink.service

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.flink.bean
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.MarketDao
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

class MarketService extends TService {
  private val marketDao = new MarketDao

  override def getDao(): TDao = marketDao

  override def analyses() = {


    val dataDS: DataStream[bean.MarketingUserBehavior] = marketDao.mockData()
    //分区到统计

    val eventDS: DataStream[bean.MarketingUserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp)
    val mapDS: DataStream[(String, Int)] = eventDS.map(data => {
      ((data.channel + "_" + data.behavior), 1)
    })


    //事件时间不用设置，由于生成数据即为当前时间
    val dataKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)


    val dataWS: WindowedStream[(String, Int), String, TimeWindow] = dataKS.timeWindow(Time.minutes(1), Time.seconds(5))

    val value: DataStream[String] = dataWS.process(new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {

      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
        val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
        out.collect(df.format(context.window.getStart) + "----" + df.format(context.window.getEnd) +"channel="+key + "访问量：" + elements.size)
      }


    })
    value


  }


}
