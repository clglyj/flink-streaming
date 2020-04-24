package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.HotItemService
import org.apache.flink.streaming.api.scala.DataStream

class HotItemController  extends   TController{

  private val hotItemService = new HotItemService

  override def execute(): Unit = {
    val resultDS: DataStream[String] = hotItemService.analyses()

    resultDS.print("wc====")
  }
}
