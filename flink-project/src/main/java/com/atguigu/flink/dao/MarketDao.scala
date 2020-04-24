package com.atguigu.flink.dao

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.atguigu.flink.bean.MarketingUserBehavior
import com.atguigu.flink.common.TDao
import com.atguigu.flink.utils.FlinkStreamEnv
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.util.Random

class MarketDao  extends   TDao{

  def mockData()={


    val env: StreamExecutionEnvironment = FlinkStreamEnv.get()
    env.addSource(new  SourceFunction[MarketingUserBehavior] {

      var runflag = true
      val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
      val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
      val rand: Random = Random
      override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {

        while (runflag) {
          val maxElements = Long.MaxValue
          var count = 0L

          while (runflag && count < maxElements) {
            val id = UUID.randomUUID().toString
            val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
            val channel = channelSet(rand.nextInt(channelSet.size))
            val ts = System.currentTimeMillis()

            ctx.collect(MarketingUserBehavior(id, behaviorType, channel, ts))
            count += 1
          }
        }
      }
      //取消标识
      override def cancel(): Unit = {
        runflag = false
      }
    })

  }
}
