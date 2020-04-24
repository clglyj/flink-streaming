package com.atguigu.test

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}


object WordCount02 {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val fileDS: DataStream[String] = env.readTextFile("input/word.txt")
    val flatmapDS: DataStream[String] = fileDS.flatMap(_.split(" "))
    val mapDS: DataStream[(String, Int)] = flatmapDS.map((_,1))
    //这里的sum属于有状态的聚集，跟有界流不同
    //sum聚集每个slot中的数据
    val resultDS: DataStream[(String, Int)] = mapDS.keyBy(0).sum(1)
    resultDS.print("==========")

    //流式框架需要
    env.execute("test---")
  }


}
