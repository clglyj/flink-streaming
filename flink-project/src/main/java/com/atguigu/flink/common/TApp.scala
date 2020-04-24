package com.atguigu.flink.common

import com.atguigu.flink.utils.FlinkStreamEnv

import scala.util.control.Breaks


//TODO 当每个类中都用共同方法时，可以声明一个具有相同函数的特质，类继承特质即可
trait TApp {

  //predef???????
  //TODO 控制抽象
  def  start(op : => scala.Unit):Unit ={
    try{
      FlinkStreamEnv.init()
      op
      FlinkStreamEnv.execute()
    }catch {
      case e => e.printStackTrace()
    }finally {
      FlinkStreamEnv.clear()
    }

  }

}
