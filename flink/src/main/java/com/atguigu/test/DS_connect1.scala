package com.atguigu.test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

object DS_connect1 {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    val intDS: DataStream[ Int] = env.fromCollection(List(
      (1),
      ( 2),
      ( 3),
      ( 2),
      ( 3),
      (4)
    ))
    val strDS: DataStream[String] = env.fromCollection(List(
      ("abc"),
      ("acb"),
      ("bcd"),
      ("arb"),
      ("bgd"),
      ("bed")
    ))


    //[Int, String] 分别是intDS和strDS数据类型
    val connDS: ConnectedStreams[Int, String] = intDS.connect(strDS)


    val res1DS: DataStream[String] = connDS.map(
      x => x.toString,
      y => y
    )
//    res1DS.print()

    val anyDS: DataStream[Any] = connDS.map(x=>x,y=>y)
    anyDS.print("==========")
    env.execute()

  }
}
