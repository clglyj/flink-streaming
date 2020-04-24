package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.OrderJoinController

object OrderJoinApplication extends App  with   TApp{

  start{
    val orderJoinController = new OrderJoinController
    orderJoinController.execute()
  }
}
