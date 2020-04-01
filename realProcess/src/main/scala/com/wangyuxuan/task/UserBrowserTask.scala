package com.wangyuxuan.task

import com.wangyuxuan.`trait`.DataProcess
import com.wangyuxuan.bean.{Message, UserBrowser, UserScan, UserState}
import com.wangyuxuan.tools.{HbaseUtils, TimeUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.hadoop.hbase.TableName

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangyuxuan
 * @date 2020/4/1 16:57
 * @description 用户上网游览器类型分析业务类
 */
object UserBrowserTask extends DataProcess {
  override def process(dataStream: DataStream[Message]): Unit = {
    // 1、将消息转换成UserBrowser实体类
    val userBrowserStream: DataStream[UserBrowser] = dataStream.flatMap(message => {
      val timestamp: Long = message.timestamp
      val userScan: UserScan = message.userScan
      val browserType: String = userScan.browserType
      val userID: String = userScan.userID
      // 获取指定时间格式
      val hour: Long = TimeUtils.getDate(timestamp, "yyyyMMddHH")
      val day: Long = TimeUtils.getDate(timestamp, "yyyyMMdd")
      val month: Long = TimeUtils.getDate(timestamp, "yyyyMM")
      // 获取用户的状态
      val userState: UserState = UserState.getUserState(userID, timestamp)
      val isNew: Boolean = userState.isNew
      val isFirstHour: Boolean = userState.isFirstHour
      val isFirstDay: Boolean = userState.isFirstDay
      val isFirstMonth: Boolean = userState.isFirstMonth
      // 构建一个数组存放UserBrowser对象
      val arrayBuffer: ArrayBuffer[UserBrowser] = ArrayBuffer[UserBrowser]()
      // 判断
      if (isNew) {
        arrayBuffer += UserBrowser(browserType, 1, 1, 0, timestamp, hour.toString)
        arrayBuffer += UserBrowser(browserType, 1, 1, 0, timestamp, day.toString)
        arrayBuffer += UserBrowser(browserType, 1, 1, 0, timestamp, month.toString)
      } else {
        // 小时
        isFirstHour match {
          case true => arrayBuffer += UserBrowser(browserType, 1, 0, 1, timestamp, hour.toString)
          case _ => arrayBuffer += UserBrowser(browserType, 1, 0, 0, timestamp, hour.toString)
        }
        // 天
        isFirstDay match {
          case true => arrayBuffer += UserBrowser(browserType, 1, 0, 1, timestamp, day.toString)
          case _ => arrayBuffer += UserBrowser(browserType, 1, 0, 0, timestamp, day.toString)
        }
        // 月
        isFirstMonth match {
          case true => arrayBuffer += UserBrowser(browserType, 1, 0, 1, timestamp, month.toString)
          case _ => arrayBuffer += UserBrowser(browserType, 1, 0, 0, timestamp, month.toString)
        }
      }
      arrayBuffer
    })
    // 2、分流
    val keyByStream: KeyedStream[UserBrowser, String] = userBrowserStream.keyBy(x => x.browser)
    // 3、划分时间窗口
    val timeWindowStream: WindowedStream[UserBrowser, String, TimeWindow] = keyByStream.timeWindow(Time.seconds(1))
    // 4、聚合统计
    val result: DataStream[UserBrowser] = timeWindowStream.reduce((u1, u2) => {
      val browser: String = u1.browser
      val timeStamp: Long = u1.timeStamp
      val dateField: String = u1.dateField
      val count1: Long = u1.count
      val newCount1: Long = u1.newCount
      val oldCount1: Long = u1.oldCount
      val count2: Long = u2.count
      val newCount2: Long = u2.newCount
      val oldCount2: Long = u2.oldCount
      UserBrowser(browser, count1 + count2, newCount1 + newCount2, oldCount1 + oldCount2, timeStamp, dateField)
    })
    // 5、数据结果落地hbase表中
    result.addSink(line => {
      val timeStamp: Long = line.timeStamp
      val browser: String = line.browser
      var count: Long = line.count
      var newCount: Long = line.newCount
      var oldCount: Long = line.oldCount
      // 定义hbase表信息
      val tableName: TableName = TableName.valueOf("device")
      val columnFamily = "info"
      val countColumn = "userBrowserCount"
      val newCountColumn = "userBrowserNewCount"
      val oldCountColumn = "userBrowserOldCount"
      // 定义rowkey
      val rowkey = TimeUtils.getDate(timeStamp, "yyyyMMddHH").toString + ":" + browser
      // hbase表中查询数据
      val countData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, countColumn)
      val newCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, newCountColumn)
      val oldCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, oldCountColumn)
      // 判断
      if (StringUtils.isNotBlank(countData)) {
        count += countData.toLong
      }
      // 判断
      if (StringUtils.isNotBlank(newCountData)) {
        newCount += newCountData.toLong
      }
      // 判断
      if (StringUtils.isNotBlank(oldCountData)) {
        oldCount += oldCountData.toLong
      }
      // 封装数据到map
      var map: Map[String, Long] = Map[String, Long]()
      map += (countColumn -> count)
      map += (newCountColumn -> newCount)
      map += (oldCountColumn -> oldCount)
      // 数据插入到hbase表中
      HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
    })
  }
}
