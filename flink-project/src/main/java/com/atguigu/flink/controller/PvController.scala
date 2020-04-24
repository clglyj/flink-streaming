package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.PvService
import org.apache.flink.streaming.api.scala.DataStream

class PvController extends   TController{

  private val pvService = new  PvService
  override def execute() = {
    val resultDS: DataStream[(String, Int)] = pvService.analyses()
    resultDS.print("========")
  }
}
