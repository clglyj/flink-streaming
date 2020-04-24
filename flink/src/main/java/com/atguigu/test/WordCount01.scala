package com.atguigu.test

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.api.scala._
object WordCount01 {


  def main(args: Array[String]): Unit = {


//    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val wordFileDS: DataSet[String] = env.readTextFile("input/word.txt")
//    val wordMapDS: DataSet[(String, Int)] = wordFileDS.flatMap(_.split(" ")).map((_,1))
//    val groupDS: GroupedDataSet[(String, Int)] = wordMapDS.groupBy(0)
//    val resultAGDS: AggregateDataSet[(String, Int)] = groupDS.sum(1)
//    resultAGDS.print()

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val fileDS: DataSet[String] = env.readTextFile("input/word.txt")

    val wordDS: DataSet[(String, Int)] = fileDS.flatMap(_.split(" ")).map((_,1))

    val resultDS: AggregateDataSet[(String, Int)] = wordDS.groupBy(0).sum(1)

    resultDS.print()
  }

}
