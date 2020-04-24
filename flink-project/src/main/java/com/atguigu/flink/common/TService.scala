package com.atguigu.flink.common

import java.text.SimpleDateFormat

import com.atguigu.flink.bean.{ResourceLog, UserItemClick}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

trait TService {

  def getDao():TDao

  def  analyses():Any

  def getHotItemClick() ={

    implicit   val path:String  = "input/UserBehavior.csv"
    val fileDS: DataStream[String] = getDao.readTextFile(path)

    val mapDS: DataStream[UserItemClick] = fileDS.map(data => {
      val datas: Array[String] = data.split(",")
      //543462,1715,1464116,pv,1511658000
      UserItemClick(
        datas(0).toLong,
        datas(1).toLong,
        datas(2).toLong,
        datas(3),
        datas(4).toLong
      )
    })

    mapDS
  }

  def getHotResource() ={
//83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
    val fileDS: DataStream[String] = getDao().readTextFile("input/apache.log")

    val dataDS: DataStream[ResourceLog] = fileDS.map(data => {
      val datas: Array[String] = data.split(" ")
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      ResourceLog(
        datas(0),
        datas(1),
        sdf.parse(datas(3)).getTime, //TODO 这里的单位是ms
        datas(5),
        datas(6)
      )
    })


    dataDS
  }


}
