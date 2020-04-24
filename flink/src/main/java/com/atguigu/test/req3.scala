package com.atguigu.test

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object req3 {

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



    val timeDS: DataStream[MyTimeDemo] = timedemoDS.assignTimestampsAndWatermarks(
      new AssignerWithPunctuatedWatermarks[MyTimeDemo] {
        override def checkAndGetNextWatermark(lastElement: MyTimeDemo, extractedTimestamp: Long): Watermark = {
          new Watermark(extractedTimestamp)
        }

        override def extractTimestamp(element: MyTimeDemo, previousElementTimestamp: Long): Long = {
          element.ts * 1000L
        }
      }
    )

    val dataKS: KeyedStream[MyTimeDemo, String] = timeDS.keyBy(_.name)

    val outTag = new OutputTag[Int]("hight")
    //监测水位5秒钟之内连续上升，连续上升，需要确定
    // TODO 注意这里的5s 不是具体的时间定义，而是日志数据中数据中时间戳间隔
    val processDS: DataStream[String] = dataKS.process(
      new KeyedProcessFunction[String, MyTimeDemo, String] {

        val outTag = new OutputTag[Int]("hight")
        //每条数据方法都会触发执行一次
        override def processElement(
                                     value: MyTimeDemo,
                                     ctx: KeyedProcessFunction[String, MyTimeDemo,
                                       String]#Context, out: Collector[String]): Unit = {

          if(value.age > 5){
            //监测数据异常流
            ctx.output(outTag,value.age)
          }else{
            out.collect("-------"+value.age)
          }

        }
      }
    )
    val outDs: DataStream[Int] = processDS.getSideOutput(outTag)
    outDs.print("=====outside-------")
    env.execute()
  }

}
