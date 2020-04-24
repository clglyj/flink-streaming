package com.atguigu.test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

object DS_connect {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    val dataDS: DataStream[(String, Int)] = env.fromCollection(List(
      ("abc", 1),
      ("acb", 2),
      ("bcd", 3),
      ("arb", 2),
      ("bgd", 3),
      ("bed", 4)
    ))



//    val resDS: KeyedStream[(String, Int), String] = dataDS.keyBy(_._1)

//    val red3: SplitStream[(String, Int)] = resDS.split(x => {
//      List("xxxx")
//    })


    val splitDS: SplitStream[(String, Int)] = dataDS.split(x => {
      val key: String = x._1
      val char: String = key.substring(1, 2)
      if (char == "c") {
        List("c")
      } else if (char == "g") {
        List("g")
      } else {
        List("xxxx")
      }
    })

//    splitDS.print("----------------")

    val cDS: DataStream[(String, Int)] = splitDS.select("c")
//    cDS.print("cccccccccccc")
    val gDS: DataStream[(String, Int)] = splitDS.select("g")
//    cDS.print("ggggggggggg")
    val xxxxDS: DataStream[(String, Int)] = splitDS.select("xxxx")
//    cDS.print("xxxxxxxx")

    val connCS: ConnectedStreams[(String, Int), (String, Int)] = cDS.connect(gDS)

    //ConnectedStreams ==> DataStream
    val mapDS: DataStream[(String, Int)] = connCS.map(x=>x ,y=>y)

    mapDS.print("=======")


    env.execute()

  }
}
