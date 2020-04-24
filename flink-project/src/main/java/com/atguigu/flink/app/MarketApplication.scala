package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.MarketController

object  MarketApplication extends  App   with   TApp{

  start{
    val marketController = new MarketController
    marketController.execute()
  }
}
