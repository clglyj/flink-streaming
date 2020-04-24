package com.atguigu.test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Sink_Redis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataStream[String] = env.fromCollection(List("a","b","c"))

    val conf: FlinkJedisPoolConfig = new  FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

    val resultDS: DataStreamSink[String] = dataDS
      .addSink(new RedisSink[String](conf,new RedisMapper[String] {
        //写入redis命令
        override def getCommandDescription: RedisCommandDescription = {
          new  RedisCommandDescription(RedisCommand.SET)
        }

        //设置key值
        override def getKeyFromData(t: String): String = {
          t
        }

        //设置的值
        override def getValueFromData(t: String): String = {

          t
        }
      }))

    env.execute("======")
  }

}
