package com.atguigu.test

import com.atguigu.test.bean.MyTimeDemo
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object req_state {

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
      */
    val timeDS: DataStream[MyTimeDemo] = timedemoDS.assignAscendingTimestamps(_.ts * 1000)

    val dataKS: KeyedStream[MyTimeDemo, String] = timeDS.keyBy(_.name)

    //监测水位5秒钟之内连续上升，连续上升，需要确定
    // TODO 注意这里的5s 不是具体的时间定义，而是日志数据中数据中时间戳间隔
    val processDS: DataStream[String] = dataKS.process(
      new KeyedProcessFunction[String, MyTimeDemo, String] {


        //TODO 为了防止数据出错后无法修复，可以使用有状态类型的计算
        //TODO 切记不能直接初始化以下两个变量，因为需要获取环境上下文，直接使用环境上下文还没有初始化
        private  var  currLevel:ValueState[Long] = _
        private  var  alertTime:ValueState[Long]  = _


        //TODO 有状态类型赋值方式有两种：
        //TODO 方法一：在open方法中进行赋值初始化，这样做可以体现该状态变量的声明周期
        //TODO 方法二：使用lazy关键字修饰变量，并定义为val类型，懒加载在使用的时候开始初始化该变量
        override def open(parameters: Configuration): Unit = {
          currLevel   =getRuntimeContext.getState(
           new ValueStateDescriptor[Long]("currLevel",classOf[Long])
          )

          alertTime = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("alertTime",classOf[Long])
          )
        }

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
          if(value.age > currLevel.value()){
            //需要注册定时器，只有在没有注册的情况下注册定时器
            if(alertTime.value() == 0L){
              //向后推送5s中触发定时器
              alertTime.update(value.ts * 1000L + 5000)
              ctx.timerService().registerEventTimeTimer(alertTime.value())
            }

          }else {
            //需要重置定时器--删除定时器
            ctx.timerService().deleteEventTimeTimer(alertTime.value())
            //重新设置定时器
            alertTime.update(value.ts * 1000 +5000)
            ctx.timerService().registerEventTimeTimer(alertTime.value())
          }
          //重置currLevel
          currLevel.update(value.age)

        }
      }
    )
    processDS.print("-----------")




    env.execute()
  }

}
