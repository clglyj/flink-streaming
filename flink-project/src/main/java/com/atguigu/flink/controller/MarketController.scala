package com.atguigu.flink.controller

import com.atguigu.flink.bean
import com.atguigu.flink.common.TController
import com.atguigu.flink.service.MarketService
import org.apache.flink.streaming.api.scala.{DataStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class MarketController  extends   TController{
  private val marketService = new MarketService
  override def execute(): Unit = {

     val resultDS: DataStream[String] = marketService.analyses()
    resultDS.print("========")
  }
}
