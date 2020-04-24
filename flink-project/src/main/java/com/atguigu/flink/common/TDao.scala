package com.atguigu.flink.common

import com.atguigu.flink.utils.FlinkStreamEnv
import org.apache.flink.streaming.api.scala.DataStream

/**
  * 通用数据访问特质
  */
trait TDao {

  /**
    * 读取文件
    *
    */
  //TODO 注意这里方法入参的处理方式值得借鉴
  //
  def  readTextFile(implicit  path:String):DataStream[String] ={
    FlinkStreamEnv.get().readTextFile(path)
  }

  /**
    * 读取kafka数据
    */
  def  readKafka:Unit={

  }

  /**
    * 读取网络socket数据
    */
  def readSocket:Unit={

  }

}
