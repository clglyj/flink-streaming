package com.atguigu.test

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object DS_reduce {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    val dataDS: DataStream[(String, Int)] = env.fromCollection(List(
      ("a", 1),
      ("a", 2),
      ("b", 3),
      ("b", 4)
    ))



    val resDS: KeyedStream[(String, Int), String] = dataDS.keyBy(_._1)

    val red1: DataStream[(String, Int)] = resDS.reduce(
      (x, y) => {
        x
      }
    )

    val red2: DataStream[(String, Int)] = resDS.reduce((x, y) => {
      (x._1, x._2 + y._2)
    })

    val red3: DataStream[(String, Int)] = resDS.reduce((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })




    red3.print("==")
    env.execute()

  }
}
