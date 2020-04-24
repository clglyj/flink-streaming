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

object Time_event_time_self_period {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * TODO 并行度问题：并行度不为1，则计算窗口时按照不同的并行度单独计算
      * 但是watermark是跨分区的，多个分区通过广播方式传递，所以会出现一个分区拿到不同
      * 分区的watermark，这个时候会选择使用较小的来使用
      *
      */




    env.setParallelism(1)
    //默认情况下采用eventtime
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)




    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",9999)
    val timedemoDS: DataStream[MyTimeDemo] = dataDS.map(x => {
      val str: Array[String] = x.split(",")
      MyTimeDemo(str(0), str(1).toLong, str(2).toInt)
    })


    /** 自定义事件事件的抽取和生成水位线watermark*/

    val resultDS: DataStream[MyTimeDemo] = timedemoDS.assignTimestampsAndWatermarks(

      //TODO 间歇性水位线生成，有数据才生成,每来一条数据生成一次watermark
      new AssignerWithPunctuatedWatermarks[MyTimeDemo] {
        override def checkAndGetNextWatermark(t: MyTimeDemo, l: Long): Watermark = {
          println("#################")
          new Watermark(l)
        }



        override def extractTimestamp(t: MyTimeDemo, l: Long): Long = {
          t.ts * 1000L
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
