package com.wangyuxuan.task

import com.wangyuxuan.`trait`.DataProcess
import com.wangyuxuan.bean.{ChannelPVUV, Message, UserState}
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
 * @date 2020/4/1 14:30
 * @description 实时频道的PV/UV统计
 */
object ChannelPVUVTask extends DataProcess {
  // 数据处理
  override def process(dataStream: DataStream[Message]): Unit = {
    // todo: 1、将数据转换为PV UV实体类
    val channelPVUVDataStream: DataStream[ChannelPVUV] = dataStream.flatMap(message => {
      // 获取时间戳
      val timestamp: Long = message.timestamp
      // 根据时间戳获取聚合的字段
      // 小时
      val hour: Long = TimeUtils.getDate(timestamp, "yyyyMMddHH")
      // 日
      val day: Long = TimeUtils.getDate(timestamp, "yyyyMMdd")
      // 月
      val month: Long = TimeUtils.getDate(timestamp, "yyyyMM")
      // 获取用户的userid
      val userID: String = message.userScan.userID
      val pvCount: Int = message.count
      // 根据用户id和时间戳获取用户访问状态
      val userState: UserState = UserState.getUserState(userID, timestamp)
      val isNew: Boolean = userState.isNew
      val firstHour: Boolean = userState.isFirstHour
      val firstDay: Boolean = userState.isFirstDay
      val firstMonth: Boolean = userState.isFirstMonth
      // 获取userScan对象中的channelId
      val channelID: String = message.userScan.channelID
      // 构建ChannelPVUV实体对象
      val arrayBuffer: ArrayBuffer[ChannelPVUV] = ArrayBuffer[ChannelPVUV]()
      // 如果是新用户
      if (isNew) {
        arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 1, timestamp, hour.toString, hour + channelID)
        arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 1, timestamp, day.toString, day + channelID)
        arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 1, timestamp, month.toString, month + channelID)
        // 如果不是新用户，看看是否是当前小时/天/月的新用户
      } else {
        // 小时
        firstHour match {
          case true => {
            arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 1, timestamp, hour.toString, hour + channelID)
          }
          case _ => {
            arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 0, timestamp, hour.toString, hour + channelID)
          }
        }
        // 日
        firstDay match {
          case true => {
            arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 1, timestamp, day.toString, day + channelID)

          }
          case _ => {
            arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 0, timestamp, day.toString, day + channelID)
          }
        }

        // 月
        firstMonth match {
          case true => {
            arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 1, timestamp, month.toString, month + channelID)

          }
          case _ => {
            arrayBuffer += ChannelPVUV(channelID, userID, pvCount, 0, timestamp, month.toString, month + channelID)
          }
        }
      }
      arrayBuffer
    })
    // todo: 2、将数据进行分流
    val groupDataStream: KeyedStream[ChannelPVUV, String] = channelPVUVDataStream.keyBy(line => line.groupField)
    // todo:3、划分时间窗口
    val windowStream: WindowedStream[ChannelPVUV, String, TimeWindow] = groupDataStream.timeWindow(Time.seconds(3))
    // todo:4、将pv和uv进行聚合
    val reduceStream: DataStream[ChannelPVUV] = windowStream.reduce((c1, c2) => {
      val channelID: String = c1.channelID
      val timeStamp: Long = c1.timeStamp
      val dateField: String = c1.dateField
      val groupField: String = c1.groupField
      val userID: String = c1.userID
      ChannelPVUV(channelID, userID, c1.pv + c2.pv, c1.uv + c2.uv, timeStamp, dateField, groupField)
    })
    // todo:5、将结果集落地到hbase表中
    reduceStream.addSink(c => {
      var pv: Long = c.pv
      var uv: Long = c.uv
      val rowkey: String = c.channelID + ":" + c.dateField
      // 数据落地到hbase的channel表中， 有可能这个表之前有对应的记录，这里需要先查询在累加
      // 先查询历史数据，然后在累加
      val tableName: TableName = TableName.valueOf("channel")
      val columnFamily = "info"
      // 表中的pv和uv结果
      val pvHbase: String = HbaseUtils.getData(tableName, rowkey, columnFamily, "pv")
      val uvHbase: String = HbaseUtils.getData(tableName, rowkey, columnFamily, "uv")
      if (StringUtils.isNotBlank(pvHbase)) {
        pv += pvHbase.toLong
      }
      if (StringUtils.isNotBlank(uvHbase)) {
        uv += uvHbase.toLong
      }
      // 数据插入到hbase表
      var map: Map[String, Long] = Map[String, Long]()
      map += ("pv" -> pv)
      map += ("uv" -> uv)
      HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
    })
  }
}
