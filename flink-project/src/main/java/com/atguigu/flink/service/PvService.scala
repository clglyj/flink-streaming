package com.atguigu.flink.service

import com.atguigu.flink.bean
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.PvDao
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class PvService  extends   TService{

  private val pvDao = new PvDao
  override def getDao(): TDao = pvDao

  override def analyses() = {

    //获取数据源
    val fileDS: DataStream[bean.UserItemClick] = getHotItemClick()

    //设置事件时间
    val timeDS: DataStream[bean.UserItemClick] = fileDS.assignAscendingTimestamps(_.timestamp * 1000)
    //过滤点击事件
    val filterDS: DataStream[bean.UserItemClick] = timeDS.filter("pv" == _.behavior)
    //对数据进行类型转换便于统计数量
    val mapDS: DataStream[(String, Int)] = filterDS.map(x => ("pv",1))


    //TODO 方案一：由于是统计的一小时的全部流量，直接使用timewindowall窗口进行计算，使用该窗口由于没有使用keyby进行
    //TODO 分流，则始终只有一个并行度计算
    val dataAWS: AllWindowedStream[(String, Int), TimeWindow] = mapDS.timeWindowAll(Time.hours(1))
    val resultDS: DataStream[(String, Int)] = dataAWS.sum(1)

    //TODO  方案二：首先对key进行归集（但是始终只有一个KEY值）
    //TODO 注意在此对window的理解，如果不经过keyBy的DS,只能使用全窗口，也就是说数据不分组，计算并行度为1
//    val dataKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)
//    val dataWS: WindowedStream[(String, Int), String, TimeWindow] = dataKS.timeWindow(Time.hours(1))

    resultDS
  }
}
