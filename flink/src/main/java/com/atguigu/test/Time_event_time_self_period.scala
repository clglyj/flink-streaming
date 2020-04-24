package com.atguigu.test

import java.text.SimpleDateFormat

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Time_event_time_self_fix {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //默认情况下采用eventtime
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



//    val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",9999)
    val timedemoDS: DataStream[MyTimeDemo] = dataDS.map(x => {
      val str: Array[String] = x.split(",")
      MyTimeDemo(str(0), str(1).toLong, str(2).toInt)
    })


    /** 自定义事件事件的抽取和生成水位线watermark*/

    val resultDS: DataStream[MyTimeDemo] = timedemoDS.assignTimestampsAndWatermarks(

      //周期性水位线，会以200ms时间间隔生成水位线数据
      new AssignerWithPeriodicWatermarks[MyTimeDemo] {

        //当前时间戳，即最新数据的时间戳
        private var currTimestamp = 0L

        //获取当前watermark
        //当被触发时生成水位线
        override def getCurrentWatermark: Watermark = {

          new Watermark(currTimestamp - 3000)
        }

        //抽取事件戳
        override def extractTimestamp(t: MyTimeDemo, l: Long): Long = {

          currTimestamp = currTimestamp.max(t.ts * 1000)
          //设置事件时间
          t.ts * 1000

        }
      }

    )





    //如果watermark已经触发了窗口计算，则这个窗口就不会再接收数据
    //如果窗口计算完毕后，还想对迟到的数据进行处理，可以让窗口接收迟到的数据，
    // 使用allowedLateness(Time.seconds(2))执行迟到时间
    val applyDS: DataStream[String] = resultDS.keyBy(_.name)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
      .apply(
        //对窗口进行数据处理
        (key: String, window: TimeWindow, datas: Iterable[MyTimeDemo], out: Collector[String]) => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val start = window.getStart
          val end = window.getEnd
          out.collect(s"[${start}-${end}), 数据[${datas}]")
        }
      )
    timedemoDS.print("mark----------")
    applyDS.print("window=======")



    env.execute()
  }

}
