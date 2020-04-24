package com.atguigu.test

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import org.apache.flink.api.scala._

object Sink_es {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataStream[String] = env.fromCollection(List("a","b","c"))



    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))


    val demoDS: DataStream[MyCaseDemo] = dataDS.map(x => {
      MyCaseDemo(x, 100, 1)
    })


    val esSinkBuilder = new ElasticsearchSink.Builder[MyCaseDemo]( httpHosts, new ElasticsearchSinkFunction[MyCaseDemo] {
      override def process(t: MyCaseDemo, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("saving data: " + t)
        val json = new util.HashMap[String, String]()
        json.put("data", t.toString)
        val indexRequest = Requests.indexRequest().index("sink-es").`type`("readingData").source(json)
        requestIndexer.add(indexRequest)
        println("saved successfully")
      }
    } )
    demoDS.addSink(esSinkBuilder.build())
    env.execute("sink_es")

  }

  case  class   MyCaseDemo(name:String,age:Int,sex:Int)

}
