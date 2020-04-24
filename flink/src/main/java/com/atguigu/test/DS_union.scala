package com.atguigu.test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

object DS_union {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    val intDS: DataStream[ Int] = env.fromCollection(List(1,2,3))
    val strDS: DataStream[String] = env.fromCollection(List("abc","acb","bcd"))
    val xyDS: DataStream[String] = env.fromCollection(List("x","y"))
    val unionDS: DataStream[String] = intDS.map(_.toString).union(strDS,xyDS)

    unionDS.print("==========")
    env.execute()

  }
}
