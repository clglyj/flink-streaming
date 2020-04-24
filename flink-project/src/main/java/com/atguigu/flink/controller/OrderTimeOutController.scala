package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.OrderTimeOutService
import org.apache.flink.streaming.api.scala.DataStream

class OrderTimeOutController  extends   TController{
  override def execute(): Unit = {
    val orderTimeOutService = new OrderTimeOutService
    val resDS: DataStream[String] = orderTimeOutService.analyses()
    resDS.print("=================")
  }
}
