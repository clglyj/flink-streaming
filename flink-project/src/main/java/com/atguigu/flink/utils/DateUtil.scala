package com.atguigu.flink.utils

import java.text.SimpleDateFormat
import java.util.Date

class DateUtil {


  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def  getDateStr(timestamp :Long) : String = {
    sdf.format(new Date(timestamp))
  }

}
