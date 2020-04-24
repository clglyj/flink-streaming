package com.atguigu.test

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
object DS_keyby {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    val dataDS: DataStream[(String, Int)] = env.fromCollection(List(
      ("a", 1),
      ("a", 2),
      ("b", 3),
      ("b", 4)
    ))


    val resultDS: KeyedStream[(String, Int), Tuple] = dataDS.keyBy(0)

    val resDS: KeyedStream[(String, Int), String] = dataDS.keyBy(_._1)
    val sumDS: DataStream[(String, Int)] = resDS.sum(1)
    val maxDS: DataStream[(String, Int)] = resDS.max(1)
    maxDS.print("----------")
    sumDS.print("==")
    env.execute()

  }
}
