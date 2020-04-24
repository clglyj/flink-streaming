package com.atguigu.flink.function

import com.atguigu.flink.bean.ClickCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UserClickWindowFunction  extends   WindowFunction[Long,ClickCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ClickCount]): Unit = {
    //out 向方法外输出数据，相当于方法返回数据
    out.collect(ClickCount(key,input.iterator.next(),window.getEnd))
  }
}
