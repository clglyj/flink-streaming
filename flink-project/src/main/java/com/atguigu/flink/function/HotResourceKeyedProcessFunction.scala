package com.atguigu.flink.function

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.flink.bean.ResourceClick
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
class HotResourceKeyedProcessFunction extends KeyedProcessFunction[Long, ResourceClick, String] {

  //TODO 注意这里不能直接进行初始化
  private var elementList: ListState[ResourceClick] = _

  private var alarm: ValueState[Long] = _


  //TODO  在open方法中初始化状态变量
  override def open(parameters: Configuration): Unit = {
    elementList = getRuntimeContext.getListState(new ListStateDescriptor[ResourceClick]("elementList", classOf[ResourceClick]))
    alarm = getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarm", classOf[Long]))
  }


  //TODO 由于窗口中数据已经混乱，需要在定时器中对数据进行排序汇总
  /**
    *
    * @param timestamp The timestamp of the firing timer   定时器执行时间
    * @param ctx       上下文信息
    * @param out       The collector for returning result values   相当于方法返回对象
    */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ResourceClick, String]#OnTimerContext, out: Collector[String]): Unit = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //将数据转换为可排序的list
    val datas: lang.Iterable[ResourceClick] = elementList.get()
//    println("=========="+datas)
     val dataIter: util.Iterator[ResourceClick] = datas.iterator()



    val list = new ListBuffer[ResourceClick]
        while (dataIter.hasNext){
          list.append(dataIter.next())
        }

//    for (d <- datas) {
//      list.append(d)
//    }

    //清除数据缓存
    elementList.clear()
    alarm.clear()

    //队列表数据进行排序
    val resultList: ListBuffer[ResourceClick] = list.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)



    // 将结果输出到控制台
    val builder = new StringBuilder
    builder.append("当前时间：" + sdf.format(new Date(timestamp)) + "\n")
    for (data <- resultList) {
      builder.append("页面链接URL：" + data.url + ", 点击数量：" + data.clickCount + "\n")
    }
    builder.append("================")
//    println("=========="+builder)
    out.collect(builder.toString())

    //    Thread.sleep(1000)

  }

  //TODO  对每一个元素进行处理
  override def processElement(value: ResourceClick, ctx: KeyedProcessFunction[Long, ResourceClick, String]#Context, out: Collector[String]): Unit = {

    elementList.add(value)
    //TODO  定时器没有初始化时需要初始化定时器
    if (alarm.value() == 0) {
      //注册定时器
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      //需要更新报警时间
      alarm.update(value.windowEndTime)
    }

  }
}




