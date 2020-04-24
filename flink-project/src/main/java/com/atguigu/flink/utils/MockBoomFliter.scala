package com.atguigu.flink.utils

/**
  * 布隆过滤器模拟对象
  * 使用redis作为位图，
  * 使用当前对象对位图进行定位处理
  */
object MockBoomFliter {


  private val cap = 1 << 29L

  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
      result = result * seed + value.charAt(i)

    }
//    println("-----------"+result)
    (cap - 1) & result
  }


//  def main(args: Array[String]): Unit = {
//    println(hash("908602",10))
//    println(hash("16080",10))
//    println(hash("954400",10))
//    println(hash("6068",10))
//  }


}
