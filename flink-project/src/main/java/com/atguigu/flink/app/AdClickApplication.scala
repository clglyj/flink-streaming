package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.AdClickController

object AdClickApplication extends  App  with   TApp{


  start{
    val adClickController = new AdClickController
    adClickController.execute()
  }
}
