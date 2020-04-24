package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.{AdClickService, AdvertClickAnalysesService}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class AdClickController  extends   TController{
  private val adClickService = new AdvertClickAnalysesService
//  private val adClickService = new AdClickService
  override def execute(): Unit = {
   val resultDS: DataStream[String] = adClickService.analyses()
    resultDS.print()

  }

}
