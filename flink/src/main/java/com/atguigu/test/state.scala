package com.atguigu.test

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object state {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
//    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",8888)
    val timedemoDS: DataStream[MyTimeDemo] = dataDS.map(x => {
      val str: Array[String] = x.split(",")
      MyTimeDemo(str(0), str(1).toLong, str(2).toInt)
    })

    val timeDS: DataStream[MyTimeDemo] = timedemoDS.assignAscendingTimestamps(_.ts * 1000)




    val dataKS: KeyedStream[MyTimeDemo, String] = timeDS.keyBy(_.name)

    //TODO 关于state状态的一些理解：
    //TODO 1.有状态就是会保存中间结果，如有异常不需要从头开始计算，
    //TODO 2.mapWithState 参数中Option的使用，为了将中间结果放置到缓冲区，getOrElse如果没有可以
    //TODO      给一个默认值，有则取出其中的值，可以很好地操作缓冲区数据，计算之后可以返回Option对象
    //TODO
    val stateDS: DataStream[MyTimeDemo] = dataKS.mapWithState((d, opt:Option[Int]) => {
      (d, Option(d.age + opt.getOrElse(0)))
    })

    stateDS.print("========")



    env.execute()
  }

}
