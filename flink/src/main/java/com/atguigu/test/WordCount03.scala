package com.atguigu.test

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import  org.apache.flink.api.scala._
object WordCount03 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(2)

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102",9999)


    val mapDS: DataStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_,1))

    val resultDS: DataStream[(String, Int)] = mapDS.keyBy(0).sum(1)

    resultDS.print("wc")
    env.execute("app")



  }
}
