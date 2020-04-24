package com.atguigu.test


import java.text.SimpleDateFormat

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Time_event_1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //默认情况下采用eventtime
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



//    val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",8888)
    val timedemoDS: DataStream[MyTimeDemo] = dataDS.map(x => {
      val str: Array[String] = x.split(",")
      MyTimeDemo(str(0), str(1).toLong, str(2).toInt)
    })
    //抽取时间戳和设定水位线
    val resultDS: DataStream[MyTimeDemo] = timedemoDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[MyTimeDemo](Time.seconds(3)) {
        override def extractTimestamp(t: MyTimeDemo): Long = {
          t.ts * 1000
        }
      }
    )
    val applyDS: DataStream[String] = resultDS.keyBy(_.name).timeWindow(Time.seconds(7))
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
