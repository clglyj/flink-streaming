package com.atguigu.test

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object req2 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",8888)
    val timedemoDS: DataStream[MyTimeDemo] = dataDS.map(x => {
      val str: Array[String] = x.split(",")
      MyTimeDemo(str(0), str(1).toLong, str(2).toInt)
    })


    /**
      * -----------> -------MyTimeDemo(sensor_1,1549044122,1)
      * -----------> -------MyTimeDemo(sensor_1,1549044126,5)
      * -----------> -------MyTimeDemo(sensor_1,1549044128,7)
      * -----------> 触发报警.......sensor_1-1549044127999
      *
      * 关于1549044127999的解释
      * public final Watermark getCurrentWatermark() {
      * return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
      * }
      *
      * //解决1549044127999的问题可以使用以下方式
      *
      *
      * 结果：
      * -----------> -------MyTimeDemo(sensor_1,1549044122,1)
      * -----------> -------MyTimeDemo(sensor_1,1549044127,6)
      * -----------> 触发报警.......sensor_1-1549044127000
      */
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

    //监测水位5秒钟之内连续上升，连续上升，需要确定
    // TODO 注意这里的5s 不是具体的时间定义，而是日志数据中数据中时间戳间隔
    val processDS: DataStream[String] = dataKS.process(
      new KeyedProcessFunction[String, MyTimeDemo, String] {

        private  var  currLevel = 0L
        private  var  alertTime  = 0L


        override def onTimer(
                              timestamp: Long,
                              ctx: KeyedProcessFunction[String, MyTimeDemo, String]#OnTimerContext,
                              out: Collector[String]): Unit = {


          out.collect(s"触发报警.......${ctx.getCurrentKey}-${ctx.timerService().currentWatermark()}")

        }

        //每条数据方法都会触发执行一次
        override def processElement(
                                     value: MyTimeDemo,
                                     ctx: KeyedProcessFunction[String, MyTimeDemo,
                                       String]#Context, out: Collector[String]): Unit = {

          out.collect("-------"+value)
          if(value.age > currLevel){
            //需要注册定时器，只有在没有注册的情况下注册定时器
            if(alertTime == 0L){
              //向后推送5s中触发定时器
              alertTime = value.ts * 1000L + 5000
              ctx.timerService().registerEventTimeTimer(alertTime)
            }

          }else {
            //需要重置定时器
            ctx.timerService().deleteEventTimeTimer(alertTime)
          }
          //重置currLevel
          currLevel = value.age

        }
      }
    )
    processDS.print("-----------")
    env.execute()
  }

}
