package com.wangyuxuan.task

import com.wangyuxuan.`trait`.DataProcess
import com.wangyuxuan.bean.{ChannelUserFreshness, Message, UserScan, UserState}
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
 * @date 2020/4/1 15:45
 * @description 实时频道的新鲜度统计
 */
object ChannelUserFreshnessTask extends DataProcess {
  /**
   * 1、将实时解析出的数据转换成新鲜度实体类
   * 2、分流
   * 3、时间窗口划分
   * 4、对频道的用户新鲜度进行聚合操作
   * 5、将结果数据进行落地操作
   *
   */
  override def process(dataStream: DataStream[Message]): Unit = {
    // 1、将实时解析出的数据转换成新鲜度实体类
    val channelUserFreshnessStream: DataStream[ChannelUserFreshness] = dataStream.flatMap(message => {
      // 获取时间戳
      val timestamp: Long = message.timestamp
      val hour: Long = TimeUtils.getDate(timestamp, "yyyyMMddHH")
      val day: Long = TimeUtils.getDate(timestamp, "yyyyMMdd")
      val month: Long = TimeUtils.getDate(timestamp, "yyyyMM")
      // 获取用户的游览记录
      val userScan: UserScan = message.userScan
      val channelID: String = userScan.channelID
      val userID: String = userScan.userID
      // 新鲜度也涉及到用户的状态
      val userState: UserState = UserState.getUserState(userID, timestamp)
      val isNew: Boolean = userState.isNew
      val firstHour: Boolean = userState.isFirstHour
      val firstDay: Boolean = userState.isFirstDay
      val firstMonth: Boolean = userState.isFirstMonth
      // 构建存储ChannelUserFreshness实体对象的数组
      val arrayBuffer: ArrayBuffer[ChannelUserFreshness] = ArrayBuffer[ChannelUserFreshness]()
      // 新用户
      if (isNew) {
        arrayBuffer += ChannelUserFreshness(channelID, 1, 0, timestamp, hour.toString, hour + channelID)
        arrayBuffer += ChannelUserFreshness(channelID, 1, 0, timestamp, day.toString, day + channelID)
        arrayBuffer += ChannelUserFreshness(channelID, 1, 0, timestamp, month.toString, month + channelID)
        // 老用户
      } else {
        // 小时
        firstHour match {
          case true => arrayBuffer += ChannelUserFreshness(channelID, 0, 1, timestamp, hour.toString, hour + channelID)
          case _ => arrayBuffer += ChannelUserFreshness(channelID, 0, 0, timestamp, hour.toString, hour + channelID)
        }
        // 天
        firstDay match {
          case true => arrayBuffer += ChannelUserFreshness(channelID, 0, 1, timestamp, day.toString, day + channelID)
          case _ => arrayBuffer += ChannelUserFreshness(channelID, 0, 0, timestamp, day.toString, day + channelID)
        }
        // 月
        firstMonth match {
          case true => arrayBuffer += ChannelUserFreshness(channelID, 0, 1, timestamp, month.toString, month + channelID)
          case _ => arrayBuffer += ChannelUserFreshness(channelID, 0, 0, timestamp, month.toString, month + channelID)
        }
        arrayBuffer
      }
    })
    // 2、分流
    val keyByStream: KeyedStream[ChannelUserFreshness, String] = channelUserFreshnessStream.keyBy(x => x.groupField)
    // 3、划分时间窗口
    val timeWindowStream: WindowedStream[ChannelUserFreshness, String, TimeWindow] = keyByStream.timeWindow(Time.seconds(3))
    // 4、对新鲜度指标进行聚合操作
    val result: DataStream[ChannelUserFreshness] = timeWindowStream.reduce((c1, c2) => {
      val groupField: String = c1.groupField
      val channelID: String = c1.channelID
      val dateFiled: String = c1.dataFiled
      val timeStamp: Long = c1.timeStamp
      // 封装结果数据为ChannelUserFreshness对象
      ChannelUserFreshness(channelID, c1.newCount + c2.newCount, c1.oldCount + c2.oldCount, timeStamp, dateFiled, groupField)
    })
    // 5、保存计算结果到hbase表中
    result.addSink(c => {
      var newCount: Long = c.newCount
      var oldCount: Long = c.oldCount
      val channelID: String = c.channelID
      val dateFiled: String = c.dataFiled
      // 构建TableName对象
      val tableName: TableName = TableName.valueOf("channel")
      // 指定列族
      val columnFamily = "info"
      // 指定列
      val newCountColumn = "newCount"
      val oldCountColumn = "oldCount"
      // 指定rowkey
      val rowkey = channelID + ":" + dateFiled
      // 先查询历史数据然后进行累加
      val newCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, newCountColumn)
      val oldCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, oldCountColumn)
      if (StringUtils.isNotBlank(newCountData)) {
        newCount += newCountData.toLong
      }
      if (StringUtils.isNotBlank(oldCountData)) {
        oldCount += oldCountData.toLong
      }
      // 封装数据到map中
      var map: Map[String, Long] = Map[String, Long]()
      map += (newCountColumn -> newCount)
      map += (oldCountColumn -> oldCount)
      // 保存结果到hbase表
      HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
    })
  }
}
