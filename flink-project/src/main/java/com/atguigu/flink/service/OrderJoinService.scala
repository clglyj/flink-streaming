package com.atguigu.flink.service

import com.atguigu.flink.bean.{OrderEvent, ReceiptEvent}
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.OrderJoinDao
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class OrderJoinService extends   TService{
  private val orderJoinDao = new OrderJoinDao

  override def getDao(): TDao = orderJoinDao

  def analysesExt(): Unit ={
    //获取数据源1
    val orderDS: DataStream[String] = orderJoinDao.readTextFile("input/OrderLog.csv")

    val orderKS: KeyedStream[OrderEvent, String] = orderDS
      .map(order => {
        val datas: Array[String] = order.split(",")
        OrderEvent(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    //获取数据源2
    val receiptDS: DataStream[String] = orderJoinDao.readTextFile("input/ReceiptLog.csv")
    val reciptKS: KeyedStream[ReceiptEvent, String] = receiptDS.map(rec => {
      val datas: Array[String] = rec.split(",")
      ReceiptEvent(datas(0), datas(1), datas(2).toLong)
    })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //双流联合
    orderKS.connect(reciptKS).process(

      new CoProcessFunction[OrderEvent,ReceiptEvent,String] {
        /**order缓存*/
        private var  orderMap:MapState[String,OrderEvent] =_
        /**tx缓存*/
        private var  receiptMap:MapState[String,ReceiptEvent] =_


        override def open(parameters: Configuration): Unit = {
          orderMap = getRuntimeContext.getMapState(
            new MapStateDescriptor[String,OrderEvent]("orderMap",classOf[String],classOf[OrderEvent])

          )
          receiptMap = getRuntimeContext.getMapState(
            new MapStateDescriptor[String,ReceiptEvent]("receiptMap",classOf[String],classOf[ReceiptEvent])

          )
        }

        /**第一个流数据处理*/
        override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, String]#Context, out: Collector[String]): Unit = {
          //这个流属于订单数据
          val tx: String = value.txId
          val receipt: ReceiptEvent = receiptMap.get(tx)
          if(receipt != null){
            out.collect(value.toString +"----" +receipt.toString)
            receiptMap.remove(tx)
          }else{
            orderMap.put(tx,value)
          }

        }

        /**第一 个流数据处理*/
        override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, String]#Context, out: Collector[String]): Unit = {
          val id: String = value.txId
          val orderEvent: OrderEvent = orderMap.get(id)
          if(orderEvent != null){
            out.collect(orderEvent.toString +"---"+value.toString)
            orderMap.remove(id)
          }else{
            receiptMap.put(id,value)
          }

        }
      }
    )
  }


  override def analyses() = {
//    analysesExt
    analyses4Join
  }


  def  analyses4Join() ={
    //获取数据源1
    val orderDS: DataStream[String] = orderJoinDao.readTextFile("input/OrderLog.csv")

    val orderKS: KeyedStream[OrderEvent, String] = orderDS
      .map(order => {
        val datas: Array[String] = order.split(",")
        OrderEvent(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    //获取数据源2
    val receiptDS: DataStream[String] = orderJoinDao.readTextFile("input/ReceiptLog.csv")
    val reciptKS: KeyedStream[ReceiptEvent, String] = receiptDS.map(rec => {
      val datas: Array[String] = rec.split(",")
      ReceiptEvent(datas(0), datas(1), datas(2).toLong)
    })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //双流联合

    orderKS.intervalJoin(reciptKS)
      .between(Time.minutes(-5),Time.minutes(5))
      .process(
        /**
          * A function that processes two joined elements and produces a single output one.
          *
          * <p>This function will get called for every joined pair of elements the joined two streams.
          * The timestamp of the joined pair as well as the timestamp of the left element and the right
          * element can be accessed through the {@link Context}.
          *
          * @param < IN1> Type of the first input
          * @param < IN2> Type of the second input
          * @param < OUT> Type of the output
          */
        new ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
          override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            out.collect(left,right)
          }
        })




  }

}
