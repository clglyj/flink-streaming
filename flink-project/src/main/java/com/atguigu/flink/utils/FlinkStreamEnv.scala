package com.atguigu.flink.utils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 使用伴生对象，使用类名直接可以访问方法
  */
object FlinkStreamEnv {


  private val envLocal = new ThreadLocal[StreamExecutionEnvironment]

  /**
    * 初始化
    */
  def  init ():StreamExecutionEnvironment ={

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    envLocal.set(env)
    env
  }

  /**
    * 获取env
    */
  def  get() :StreamExecutionEnvironment ={
    var env: StreamExecutionEnvironment = envLocal.get()
    if(env == null){
      env = init()
    }
    env
  }

  /**
    * 执行
    */
  def execute() :Unit = {
    get().execute("============")
  }


  def  clear() :Unit  = {

    envLocal.remove()
  }


}
