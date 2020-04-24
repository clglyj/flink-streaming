package com.atguigu.test

import java.text.SimpleDateFormat

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Time_event {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //默认情况下采用eventtime
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val mapDS: DataStream[MyTimeDemo] = dataDS.map(
      x => {
        val datas: Array[String] = x.split(",")
        MyTimeDemo(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    // 抽取时间戳和设定水位线（标记）
    // 1. 从数据中抽取数据作为事件时间
    // 2. 设定水位线标记，这个标记一般比当前数据事件时间要推迟

    // 1549044122000 => wm: 1549044125000
    val markDS: DataStream[MyTimeDemo] = mapDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[MyTimeDemo](Time.seconds(3)) {
        // 抽取事件时间,以毫秒为单位
        override def extractTimestamp(element: MyTimeDemo): Long = {
          element.ts * 1000L
        }
      }
    )


    // 1. 时间窗口如何划分？
    //    timestamp - (timestamp - offset(0) + windowSize) % windowSize;
    //  TODO 时间戳1549044122   1549044122 -(1549044122 +5)%5 = 1549044122 -2 = 1549044120
    //  TODO 时间戳1549044122   1549044122 -(1549044122 +4)%4 = 1549044122 -2 = 1549044120
    //  TODO 时间戳1549044122   1549044122 -(1549044122 +7)%7 = 1549044122 -3 = 1549044119
    //    1Min => 5 => 12段 (前闭后开)
    //    [00:00 - 00:05)
    //    [00:05 - 00:10)
    //    [00:10 - 00:15)
    //    [00:15 - 00:20)
    //    [00:20 - 00:25)
    //    [00:25 - 00:30)
    // 2. 标记何时触发窗口计算
    //    当标记(事件时间+推迟时间)大于窗口结束的时候就会触发窗口的计算
    //   5+3 => 8s
    //   10+3 => 13s

    //TODO  窗口推迟时间和延迟时间其实不是真正的时间概念，而是数据之间的时间差

    val applyDS: DataStream[String] = markDS
      .keyBy(_.name)
      .timeWindow(Time.seconds(5))
      .apply(
        // 对窗口进行数据处理
        // key : 分流的key
        // window : 当前使用窗口的类型
        // datas : 窗口中的数据
        // out : 输出,指定输出的类型
        (key: String, window: TimeWindow, datas: Iterable[MyTimeDemo], out: Collector[String]) => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val start = window.getStart
          val end = window.getEnd
          out.collect(s"[${start}-${end}), 数据[${datas}]")
        }
      )


    markDS.print("mark>>>")
    applyDS.print("window>>>")


    env.execute()
  }

}
