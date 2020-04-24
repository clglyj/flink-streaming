package com.atguigu.test

import com.atguigu.test.bean.MyCaseTest
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import  org.apache.flink.api.scala._
object Source_File {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataDS: DataStream[String] = env.readTextFile("input")


//
//    dataDS.flatMap(_.split(" ")).map(x=>{
//      MyCaseTest(x,1,1)
//    }).print("======")

    dataDS.map(x=>{
      val arr: Array[String] = x.split(" ")
      MyCaseTest(arr(0),arr(1).toInt,arr(2).toInt)
    }).print()
    env.execute()

  }

}
