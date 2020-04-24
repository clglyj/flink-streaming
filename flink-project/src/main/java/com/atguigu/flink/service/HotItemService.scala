package com.atguigu.flink.service

import com.atguigu.flink.bean
import com.atguigu.flink.bean.UserItemClick
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.HotItemDao
import com.atguigu.flink.function.{UserClickAggregateFunction, UserClickKeyedProcessFunction, UserClickWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotItemService extends   TService{

  private val hotItemDao = new  HotItemDao

  override def analyses() = {

    //从文件读取数据
    val dataDS: DataStream[UserItemClick] = getHotItemClick()

    //过滤pv行为
    val filterDS: DataStream[UserItemClick] = dataDS.filter(_.behavior == "pv")
    
    //设置事件时间定义
    //TODO  由于给定数据是有序数据可以使用有序
    val timeDS: DataStream[UserItemClick] = filterDS.assignAscendingTimestamps(_.timestamp * 1000L)


    //根据商品ID 进行聚合
    val timeKS: KeyedStream[UserItemClick, Long] = timeDS.keyBy(_.itemId)

    //设置滚动窗口
    val timeWS: WindowedStream[UserItemClick, Long, TimeWindow] = timeKS.timeWindow(Time.hours(1L),Time.minutes(5))

    //对数据进行聚合


    //TODO
    //当对窗口数据进行聚合后窗口数据会被打乱，
    val aggreDS: DataStream[bean.ClickCount] = timeWS.aggregate(
      new UserClickAggregateFunction,
      new UserClickWindowFunction
    )

    //TODO 由于数据窗口中数据已经被打乱，则需要使用windowend字段进行归集
    val aggreKS: KeyedStream[bean.ClickCount, Long] = aggreDS.keyBy(_.windowEndTime)

    //对数据进行筛选排序
    val resultDS: DataStream[String] = aggreKS.process(new UserClickKeyedProcessFunction)



    resultDS





  }

  override def getDao(): TDao = hotItemDao
}
