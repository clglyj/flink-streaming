package com.atguigu.test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DS_filter {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val dataDS: DataStream[List[Int]] = env.fromCollection(List(
      List(1, 3, 5, 7),
      List(2, 4, 6, 8)
    ))
    //flatmap是对集合中的每一个元素进行处理，最终返回一个单个元素的集合
    val resultDS: DataStream[Int] = dataDS.flatMap(l => l)
    resultDS.filter(_ % 2 == 0).print("=======")
    env.execute()

  }

}
