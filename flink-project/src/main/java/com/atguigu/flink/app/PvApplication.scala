package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.PvController

/**
  * 需求三：网站独立访客数统计
  */
object PvApplication extends  App with TApp{

  start{
    val pvController = new  PvController
    pvController.execute()

  }

}
