package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.OrderJoinService

class OrderJoinController  extends   TController{
  override def execute(): Unit = {
    val orderJoinService = new OrderJoinService
    val resultDS= orderJoinService.analyses()
    resultDS.print("==========")
  }
}
