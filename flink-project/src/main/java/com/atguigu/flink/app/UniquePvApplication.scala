package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.UniquePvController

object  UniquePvApplication extends  App   with   TApp{

  start{
    val uniquePvController = new UniquePvController
    uniquePvController.execute()

  }


}
