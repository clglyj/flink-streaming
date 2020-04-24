package com.atguigu.flink.service

import com.atguigu.flink.bean.{OrderEvent, OrderResult}
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.OrderTimeOutDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class OrderTimeOutService  extends   TService{
  private val orderTimeOutDao = new OrderTimeOutDao
  override def getDao(): TDao = orderTimeOutDao



  override def analyses() = {

    val dataDS: DataStream[String] = orderTimeOutDao.readTextFile("input/OrderLog.csv")

    val orderDS: DataStream[OrderEvent] = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      OrderEvent(
        datas(0).toLong,
        datas(1),
        datas(2),
        datas(3).toLong
      )
    })

    val orderKS: KeyedStream[OrderEvent, Long] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L).keyBy(_.orderId)


    orderKS.process(new  KeyedProcessFunction[Long,OrderEvent,String] {

      private var orderMemberCache :ValueState[OrderResult] =_
      private var alert :ValueState[Long] =_


      override def open(parameters: Configuration): Unit = {
        orderMemberCache = getRuntimeContext.getState(
          new ValueStateDescriptor[OrderResult]("orderMemberCache",classOf[OrderResult])
        )
        alert = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("alert",classOf[Long])
        )
      }


      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {

      }

      override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]): Unit = {

        //能够进来的数据之分为两种情况，要么是支付数据，要么是下单数据，所以需要分情况而定

        var orderMember: OrderResult = orderMemberCache.value()

        val eventType: String = value.eventType

        if(eventType == "create"){
          if(orderMember == null){
            //pay数据没有来,将create数据放入缓存
            orderMember = OrderResult(value.orderId,value.eventTime,0L)
            orderMemberCache.update(orderMember)

            //TODO 注册定时器,等待pay数据到来
            ctx.timerService().registerEventTimeTimer((value.eventTime+ 15 * 60) * 1000)
            alert.update((value.eventTime+ 15 * 60) * 1000)

          }else{
            //pay数据已经来了，直接输出，并删除定时器
            // pay数据已经来了
            var s = "订单ID ：" + orderMember.orderId
            s += "支付耗时"+math.abs(orderMember.end-value.eventTime)+"秒"
            out.collect(s)

            ctx.timerService().deleteEventTimeTimer(alert.value())
            alert.clear()
            orderMemberCache.clear()

          }
        }
        if(eventType == "pay"){
          if(orderMember == null){
            //create数据没来，将pay数据缓存
            orderMember = OrderResult(value.orderId,value.eventTime,0L)
            orderMemberCache.update(orderMember)
            //TODO 注册定时器,等待create数据到来
            ctx.timerService().registerEventTimeTimer((value.eventTime+ 15 * 60) * 1000)
            alert.update((value.eventTime+ 15 * 60) * 1000)

          }else {
            //create数据已经来了，直接输出，并删除定时器
            var s = "订单ID ：" + orderMember.orderId
            s += "支付耗时"+math.abs(orderMember.end-value.eventTime)+"秒"
            out.collect(s)
            ctx.timerService().deleteEventTimeTimer(alert.value())
            alert.clear()
            orderMemberCache.clear()
          }
        }



      }
    })


  }


  def analyses4CES(): Unit ={
    val dataDS: DataStream[String] = orderTimeOutDao.readTextFile("input/OrderLog.csv")

    val orderDS: DataStream[OrderEvent] = dataDS.map(data => {
      val datas: Array[String] = data.split(",")
      OrderEvent(
        datas(0).toLong,
        datas(1),
        datas(2),
        datas(3).toLong
      )
    })

    val orderKS: KeyedStream[OrderEvent, Long] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L).keyBy(_.orderId)


    //定义规则
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("followed")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))


    //应用规则
    val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS,pattern)

    //获取结果
    val resDS: DataStream[String] = orderPS.select(map => {
      //      map.toString()
      val create: OrderEvent = map("begin").iterator.next()
      val pay: OrderEvent = map("followed").iterator.next()
      create.orderId + "耗时"+(pay.eventTime -  create.eventTime)
    })

    resDS
  }




}
