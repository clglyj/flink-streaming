package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.OrderTimeOutController

object OrderTimeOutApplication extends App with TApp   {

  start{
    val orderTimeOutController = new OrderTimeOutController
    orderTimeOutController.execute()
  }
}
