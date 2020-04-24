package com.atguigu.test

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

object DS_function {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)


    val dataDS: DataStream[Int] = env.fromCollection(List(1,2,3))

    val mapDS: DataStream[Any] = dataDS.map(x =>x * 2)
    mapDS.print()


    //通过java  Interface的方式进行结构变换
    dataDS.map(new MyMapFunction1).print("========")

    env.execute()

  }


  //public interface MapFunction<T, O> 输入输出类型
  class  MyMapFunction  extends   MapFunction[Int,Int] {
    override def map(t: Int): Int = {
      t*3
    }
  }
  //public interface MapFunction<T, O> 输入输出类型
  class  MyMapFunction1  extends   MapFunction[Int,String] {
    override def map(t: Int): String = {
      t*3 +"==="
    }
  }
}
