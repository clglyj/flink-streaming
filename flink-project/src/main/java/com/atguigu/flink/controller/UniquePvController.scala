package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.UniquePvService
import org.apache.flink.streaming.api.scala.DataStream

class UniquePvController  extends   TController{

  private val uniquePvService = new UniquePvService
  override def execute(): Unit = {

//    val resultDS: DataStream[String] = uniquePvService.analyses()
    val resultDS: DataStream[String] = uniquePvService.analysesExtByBloomFilter()
    resultDS.print()
  }
}
