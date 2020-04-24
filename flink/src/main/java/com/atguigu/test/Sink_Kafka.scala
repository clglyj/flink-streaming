package com.atguigu.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import  org.apache.flink.api.scala._

object Sink_Kafka {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataStream[String] = env.fromCollection(List("a","b","c"))


    val resultDS: DataStreamSink[String] = dataDS
      .addSink(new FlinkKafkaProducer011[String]
      ("hadoop102:9092",
        "sink_kafka", new SimpleStringSchema()))

    env.execute("======")
  }

}
