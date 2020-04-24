package com.atguigu.flink.function

import com.atguigu.flink.bean.{ResourceClick, ResourceLog}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class HotResourceWindowFunction extends WindowFunction[Long, ResourceClick, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[ResourceClick]): Unit = {
    out.collect(ResourceClick(key, input.iterator.next(), window.getEnd))
  }
}
