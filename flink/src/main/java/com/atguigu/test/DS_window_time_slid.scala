package com.atguigu.test

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object DS_window_time_slid {


  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val mapDS: DataStream[(String, Int)] = dataDS.map((_,1))

    val dataKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

    val dataWS: WindowedStream[(String, Int), String, TimeWindow] =
      dataKS.timeWindow(
        Time.seconds(3),
        Time.seconds(2)
      )

    val resultDS: DataStream[(String, Int)] = dataWS.reduce((x, y) => {
      (x._1, y._2 + x._2)
    })
    resultDS.print("wind>>>>>")




    env.execute("=========")





  }

}
