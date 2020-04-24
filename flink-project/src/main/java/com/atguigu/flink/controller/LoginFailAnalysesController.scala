package com.atguigu.flink.controller

import com.atguigu.flink.common.TController
import com.atguigu.flink.service.LoginFailAnalysesService

class LoginFailAnalysesController  extends   TController{
  override def execute(): Unit = {
    val loginFailAnalysesService = new LoginFailAnalysesService
    val resultDS = loginFailAnalysesService.analyses()
    resultDS.print("LoginFailAnalysesController========")
  }
}
