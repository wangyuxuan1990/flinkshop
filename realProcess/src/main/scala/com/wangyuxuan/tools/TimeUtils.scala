package com.wangyuxuan.tools

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * @author wangyuxuan
 * @date 2020/4/1 14:15
 * @description 时间处理的工具类
 */
object TimeUtils {
  // 定义一个获取不同格式时间的时间戳  小时--yyyyMMddHH  天---yyyyMMdd  月---yyyyMM
  def getDate(timeStamp: Long, timeFormat: String): Long = {
    // 将时间戳转换为日期
    val date: Date = new Date(timeStamp)
    // 基于不同的时间格式构建FastDateFormat格式类
    val fastDateFormat: FastDateFormat = FastDateFormat.getInstance(timeFormat)
    val time: String = fastDateFormat.format(date)
    time.toLong
  }

  // 比较2个时间戳
  def compare(t1: Long, t2: Long, format: String): Boolean = {
    getDate(t1, format) < getDate(t2, format)
  }
}
