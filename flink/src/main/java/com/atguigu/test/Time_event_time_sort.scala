package com.atguigu.test

import java.text.SimpleDateFormat

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Time_event_time_sort {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //默认情况下采用eventtime
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",9999)
    val timedemoDS: DataStream[MyTimeDemo] = dataDS.map(x => {
      val str: Array[String] = x.split(",")
      MyTimeDemo(str(0), str(1).toLong, str(2).toInt)
    })


    // TODO 如果已知数据是有序的，可以使用以下这种方式设置
    val resultDS: DataStream[MyTimeDemo] = timedemoDS.assignAscendingTimestamps(_.ts * 1000L)
    //如果watermark已经触发了窗口计算，则这个窗口就不会再接收数据
    //如果窗口计算完毕后，还想对迟到的数据进行处理，可以让窗口接收迟到的数据，
    // 使用allowedLateness(Time.seconds(2))执行迟到时间
    val applyDS: DataStream[String] = resultDS.keyBy(_.name)
      .timeWindow(Time.seconds(5))
//      .allowedLateness(Time.seconds(2))
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
