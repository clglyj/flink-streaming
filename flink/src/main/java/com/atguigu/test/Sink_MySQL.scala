package com.atguigu.test

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Sink_MySQL {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataStream[String] = env.fromCollection(List("a","b","c"))

    dataDS.addSink(new  MySQLSink)
    env.execute("======")
  }

  class MySQLSink  extends   RichSinkFunction[String] {

    var  conn:Connection =  null
    var ps:PreparedStatement = _


    override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
      ps.setString(1, "zzz")
      ps.execute()
    }

    override def open(parameters: Configuration): Unit = {

      super.open(parameters)

      conn = DriverManager.getConnection("jdbc:mysql://hadoop104:3306/rdd", "root", "123456")
      ps  = conn.prepareStatement("INSERT INTO sink_mysql (id) VALUES (?)")
    }

    override def close(): Unit = {
      if(ps != null)
      ps.close()
      if(conn != null)
      conn.close()
    }
  }

}
