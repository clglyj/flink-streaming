package com.atguigu.flink.service

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.flink.bean
import com.atguigu.flink.common.{TDao, TService}
import com.atguigu.flink.dao.UniquePvDao
import com.atguigu.flink.utils.MockBoomFliter
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import redis.clients.jedis.Jedis

import scala.collection.mutable

class UniquePvService extends TService{

  private val uniquePvDao = new UniquePvDao
  override def getDao(): TDao = uniquePvDao


  def  analysesExtByBloomFilter() = {


    //获取数据
    val fileDS: DataStream[bean.UserItemClick] = getHotItemClick()
    //变换结构
    val dataAWS: AllWindowedStream[(Long, Int), TimeWindow] = fileDS.filter(_.behavior == "pv")

      .assignAscendingTimestamps(_.timestamp * 1000)
      .map(data => {
        (data.userId, 1)
      })
      .timeWindowAll(Time.hours(1))


    //TODO 如果使用process方法，会将窗口所有的数据放置到内存中，非单独处理数据
//    dataAWS.process()
    //TODO 累加器只能以用于单独数据的累计，不能做复杂的业务逻辑处理，比如，在输出的时候
    //只能输出结果，但是不能对结果进行拼接加工
//    dataAWS.aggregate()


    //TODO  Trigger的意义就是为了指定数据数据采用什么样的方式触发
    val triggerDS: AllWindowedStream[(Long, Int), TimeWindow] = dataAWS.trigger(

      new Trigger[(Long, Int), TimeWindow] {
        //元素进入窗口触发    指定数据保留方式
        override def onElement(element: (Long, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.FIRE_AND_PURGE
        }

        //事件时间触发，数据继续，不做任何处理  已经不使用时间来计算
        override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          //
          TriggerResult.CONTINUE
        }

        //事件时间触发，数据继续，不做任何处理  已经不使用时间来计算
        override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

        }
      }
    )




    triggerDS.process(new  ProcessAllWindowFunction[(Long, Int),String,TimeWindow] {

      override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {


        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val bitKey: String = sdf.format(new Date(context.window.getEnd))


        val data: (Long, Int) = elements.iterator.next()

        val jedis = new Jedis("hadoop102",6379)

        //获取userID偏移量
        val hash: Long = MockBoomFliter.hash(data._1.toString,20)

        //根据偏移量判断用户ID在redis中是否存在
        val flag: lang.Boolean = jedis.getbit(bitKey,hash )
        println(elements + "-------------"+hash +"----------"+flag)
        //如果位图中存在，则什么都不做
        if(!flag) {
          jedis.setbit(bitKey,hash,true)
          jedis.hincrBy("uvcount",bitKey,1L)
        }

        //如果位图中不存在，则累加1




      }


    })

  }

  override def analyses() = {

    //获取数据
    val fileDS: DataStream[bean.UserItemClick] = getHotItemClick()
    //变换结构
    val dataAWS: AllWindowedStream[(Long, Int), TimeWindow] = fileDS.filter(_.behavior == "pv")

      .assignAscendingTimestamps(_.timestamp * 1000)
      .map(data => {
        (data.userId, 1)
      })
      .timeWindowAll(Time.hours(1))

    dataAWS
      .process(
        /**
          * Base abstract class for functions that are evaluated over keyed (grouped)
          * windows using a context for retrieving extra information.
          *
          * @tparam IN The type of the input value.
          * @tparam OUT The type of the output value.
          * @tparam W The type of the window.
          */
        new ProcessAllWindowFunction[(Long, Int),String,TimeWindow] {

          /**
            * Evaluates the window and outputs none or several elements.
            *
            * @param context  The context in which the window is being evaluated.
            * @param elements The elements in the window being evaluated.
            * @param out      A collector for emitting elements.
            * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
            */
          override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {

            //将窗口中的数据进行去重
            val  set = mutable.Set[Long]()

            val iter: Iterator[(Long, Int)] = elements.iterator
            while (iter.hasNext){
              set.add(iter.next()._1)
            }

            val  builder  = new mutable.StringBuilder()
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


            builder.append("time"+sdf.format(new Date(context.window.getEnd).getTime) +"\n")

            builder.append("网站独立访问统计"+set.size)
            builder.append("--------------------")
            out.collect(builder.toString())
          }
        }
      )







  }
}
