package com.atguigu.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaSource {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop102:9092")
    prop.setProperty("group.id", "consumer-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("flink-kafka-source", new SimpleStringSchema(), prop))
    val demoDS: DataStream[MyCaseDemo] = kafkaDS.map(str => {
      val line: Array[String] = str.split(" ")
      MyCaseDemo(line(0), line(1).toInt, line(2).toInt)
    })
    demoDS.print()
    env.execute("--------")





  }

}

case  class   MyCaseDemo(name:String,age:Int,sex:Int)
