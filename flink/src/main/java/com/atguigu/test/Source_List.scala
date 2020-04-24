package com.atguigu.test

import com.atguigu.test.bean.MyCaseTest
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import  org.apache.flink.api.scala._
object Source_List {

  def main(args: Array[String]): Unit = {



    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    val dataDS: DataStream[(String, Int, Int)] = env.fromCollection(List(
      ("abc", 1, 1),("bcd", 2, 2),("cde", 3, 3)
    ))
    dataDS.map( x=>{
      MyCaseTest(x._1,x._2,x._3)
    }).print("-----")


    env.execute("list===")



  }

}
