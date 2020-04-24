package com.atguigu.flink.function

import com.atguigu.flink.bean.{ClickCount, UserItemClick}
import org.apache.flink.api.common.functions.AggregateFunction

class UserClickAggregateFunction  extends   AggregateFunction[UserItemClick,Long,Long]{

  //TODO  创建变量即初始化变量
  override def createAccumulator(): Long = 0L

  //TODO  计算逻辑
  override def add(value: UserItemClick, accumulator: Long): Long = accumulator + 1L

  //TODO 获取计算结果
  override def getResult(accumulator: Long): Long =  accumulator

  //TODO 元素之间计算逻辑
  override def merge(a: Long, b: Long): Long = a + b
}
