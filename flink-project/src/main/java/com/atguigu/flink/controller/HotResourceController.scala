package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.HotResourceService
import org.apache.flink.streaming.api.scala.DataStream

class HotResourceController  extends   TController{

  private val hotResourceService = new HotResourceService

  override def execute(): Unit = {
     val resultDS: DataStream[String] = hotResourceService.analyses()

    resultDS.print("===========")
  }
}
