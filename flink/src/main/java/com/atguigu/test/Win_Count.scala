package com.atguigu.test

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Win_Count {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102",9999)





    env.execute("count")



  }
}
