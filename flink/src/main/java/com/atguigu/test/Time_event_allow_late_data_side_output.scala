package com.atguigu.test

import java.text.SimpleDateFormat

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Time_event_allow_late_data_side_output {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //默认情况下采用eventtime
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



    //如果数据源为读取文件，那么在数据读取完毕后进行计算
    //因为flink会在文件读取完毕后，将watermark设置为long的最大值，将所有窗口数据进行计算
//    val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",9999)
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

    val output = new OutputTag[MyTimeDemo]("lateData")
    //如果watermark已经触发了窗口计算，则这个窗口就不会再接收数据
    //如果窗口计算完毕后，还想对迟到的数据进行处理，可以让窗口接收迟到的数据，
    // 使用allowedLateness(Time.seconds(2))执行迟到时间
    //使用sideOutputLateData将迟到数据放入到侧输出流中
    val applyDS: DataStream[String] = resultDS.keyBy(_.name)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(output)
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
    //取出侧输出流,测输出流输出的时机？
    val outputDS: DataStream[MyTimeDemo] = applyDS.getSideOutput(output)
    outputDS.print("output>>>>>>>>>")


    env.execute()
  }

}
