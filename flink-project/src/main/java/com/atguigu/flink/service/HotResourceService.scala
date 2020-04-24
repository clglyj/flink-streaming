package com.atguigu.flink.service

import com.atguigu.flink.bean
import com.atguigu.flink.bean.ResourceLog
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.HotResourceDao
import com.atguigu.flink.function.{HotResourceAggregateFunction, HotResourceKeyedProcessFunction, HotResourceWindowFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotResourceService  extends   TService{

  private val hotResourceDao = new HotResourceDao
  override def getDao(): TDao = hotResourceDao

  override def analyses() = {

    //获取数据
    val dataDS: DataStream[bean.ResourceLog] = getHotResource()

    //使用方法名进行分组
//    val dataKS: KeyedStream[ResourceLog, String] = dataDS.keyBy(_.method)
    //设置水位线为1min
    val timeDS: DataStream[ResourceLog] = dataDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ResourceLog](Time.minutes(1)) {
        override def extractTimestamp(element: ResourceLog): Long = {
          element.eventTime
        }
      }
    )

    //TODO 相当于单条数据
    val dataKS: KeyedStream[ResourceLog, String] = timeDS.keyBy(_.url)
    //TODO timewindow会将整个窗口中的数据拿到之后统一处理
    val dataWS: WindowedStream[ResourceLog, String, TimeWindow] = dataKS.timeWindow(Time.minutes(10),Time.seconds(5))

    //对窗口数据进行初步聚合，但是聚合后窗口数据就打乱了

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually.
      * The output of the window function is  interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given aggregation function.
      *
      * @param preAggregator The aggregation function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    val aggDS: DataStream[bean.ResourceClick] = dataWS.aggregate(
      new HotResourceAggregateFunction,
      new HotResourceWindowFunction
    )

//    dataWS.reduce()

    //
    val aggKS: KeyedStream[bean.ResourceClick, Long] = aggDS.keyBy(_.windowEndTime)


//    aggKS.print("===========")
    val resultDS: DataStream[String] = aggKS.process(new HotResourceKeyedProcessFunction)

    resultDS
  }
}
