package com.atguigu.test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream, _}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

object DS_time_event {


  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val mapDS: DataStream[(String, Int)] = dataDS.map((_,1))

    val dataKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

    //滑动窗口   根据keyby后的key进行统计，当数量达到窗口大小时触发计算
    //滑动窗口 根据滑动幅度触发计算，但是计算范围仍然是窗口大小d
    val dataWS: WindowedStream[(String, Int), String, GlobalWindow] =
    dataKS.countWindow(3,2)


    val resultDS: DataStream[(String, Int)] = dataWS.reduce((x, y) => {
      (x._1, x._2 + y._2)
    })

    resultDS.print("count>>>>>")




    env.execute("=========")





  }

}
