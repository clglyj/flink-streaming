package com.atguigu.test

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DS_function_rich {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val dataDS: DataStream[Int] = env.fromCollection(List(1,2,3))

//    val mapDS: DataStream[Any] = dataDS.map(x =>x * 2)

    val mapDS: DataStream[String] = dataDS.map(new MyRichFunction)
    mapDS.print()
    env.execute()

  }

  /**
    * RichMapFunction<IN, OUT>   输入和数据数据类型
    */
  class  MyRichFunction  extends   RichMapFunction[Int,String]{


    //生命周期方法
    override def open(parameters: Configuration): Unit = super.open(parameters)
    override def close(): Unit = super.close()



    override def map(in: Int): String = {
      getRuntimeContext.getTaskName + in *2
    }
  }



}
