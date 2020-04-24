package com.atguigu.flink.function

import java.{lang, util}

import com.atguigu.flink.bean.ClickCount
import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * * @param <K> Type of the key.
  * * @param <I> Type of the input elements.
  * * @param <O> Type of the output elements.
  */
class UserClickKeyedProcessFunction extends  KeyedProcessFunction[Long, ClickCount,String]{

  //TODO 注意这里不能直接进行初始化
  private  var  elementList:  ListState[ClickCount] = _

  private  var  alarm  :ValueState[Long] = _


  //TODO  在open方法中初始化状态变量
  override def open(parameters: Configuration): Unit = {
    elementList =getRuntimeContext.getListState(new ListStateDescriptor[ClickCount]("elementList",classOf[ClickCount]))
    alarm = getRuntimeContext.getState(new  ValueStateDescriptor[Long]("alarm",classOf[Long]))
  }



  //TODO 由于窗口中数据已经混乱，需要在定时器中对数据进行排序汇总
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ClickCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //将数据转换为可排序的list
     val datas: lang.Iterable[ClickCount] = elementList.get()
    val dataIter: util.Iterator[ClickCount] = datas.iterator()

    val list = new ListBuffer[ClickCount]
    while (dataIter.hasNext){
      list.append(dataIter.next())
    }

    //清除数据缓存
    elementList.clear()
    alarm.clear()

    //队列表数据进行排序
    val resultList: ListBuffer[ClickCount] = list.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

    // 将结果输出到控制台
    val builder = new StringBuilder
    builder.append("当前时间：" + new Timestamp(timestamp) + "\n")
    for ( data <- resultList ) {
      builder.append("商品：" + data.itemId + ", 点击数量：" + data.clickCount + "\n")
    }
    builder.append("================")

    out.collect(builder.toString())

//    Thread.sleep(1000)

  }

  //TODO  对每一个元素进行处理
  override def processElement(value: ClickCount, ctx: KeyedProcessFunction[Long, ClickCount, String]#Context, out: Collector[String]): Unit = {

    elementList.add(value)
    //TODO  定时器没有初始化时需要初始化定时器
    if(alarm.value() == 0){
      //注册定时器
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      alarm.update(value.windowEndTime)
    }

  }
}




