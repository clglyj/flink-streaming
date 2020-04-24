package com.atguigu.test

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import   org.apache.flink.api.scala._
object DS_map {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(3)
    val dataDS: DataStream[Int] = env.fromCollection(List(1,2,3,4,5))

    val resultDs: DataStream[Int] = dataDS.map(n =>n *2 )
    resultDs.print("=========")


    env.execute()



  }

}
