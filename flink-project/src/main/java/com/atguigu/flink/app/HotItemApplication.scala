package com.atguigu.flink.app

import com.atguigu.flink.common.TApp
import com.atguigu.flink.controller.HotItemController

/**
  * application 应用类，需要继承App类,APP特质中包含main方法
  */

object HotItemApplication extends  App with   TApp{


  start {
    //TODO 在flink中可以使用ParameterTool获取命令行参数
//    val paramValue: String = ParameterTool.fromArgs(args).get("paramKey")
    val itemController = new HotItemController
    itemController.execute()
  }

}
