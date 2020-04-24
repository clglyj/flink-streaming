package com.atguigu.test


import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object process_keyed {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS: DataStream[String] = env.socketTextStream("hadoop102",8888)
    val timedemoDS: DataStream[MyTimeDemo] = dataDS.map(x => {
      val str: Array[String] = x.split(",")
      MyTimeDemo(str(0), str(1).toLong, str(2).toInt)
    })




    val dataKS: KeyedStream[MyTimeDemo, String] = timedemoDS.keyBy(_.name)

    val processDS: DataStream[String] = dataKS.process(
      new KeyedProcessFunction[String, MyTimeDemo, String] {


        override def onTimer(
                              timestamp: Long,
                              ctx: KeyedProcessFunction[String, MyTimeDemo, String]#OnTimerContext,
                              out: Collector[String]): Unit = {
          out.collect("timer  start ..........")
        }

        //每条数据方法都会触发执行一次
        override def processElement(
                                     value: MyTimeDemo,
                                     ctx: KeyedProcessFunction[String, MyTimeDemo,
                                       String]#Context, out: Collector[String]): Unit = {


          out.collect("key-process......." + ctx.timerService().currentProcessingTime())
          ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())

        }
      }
    )
    processDS.print("-----------")




    env.execute()
  }

}
