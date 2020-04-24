package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.LoginFailAnalysesController

object LoginFailAnalysesApplication  extends App  with   TApp{

  start{
    val loginFailAnalysesController = new LoginFailAnalysesController
    loginFailAnalysesController.execute()
  }
}
