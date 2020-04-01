package com.wangyuxuan.task

import com.wangyuxuan.`trait`.DataProcess
import com.wangyuxuan.bean.{ChannelRegion, Message, UserScan, UserState}
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
 * @date 2020/4/1 16:23
 * @description 实时频道地域分析
 */
object ChannelRegionTask extends DataProcess {
  override def process(dataStream: DataStream[Message]): Unit = {
    // 1、将消息的实体类 转换成频道地域样例类
    val channelRegionStream: DataStream[ChannelRegion] = dataStream.flatMap(message => {
      // 获取时间戳的年月日
      val timestamp: Long = message.timestamp
      val hour: Long = TimeUtils.getDate(timestamp, "yyyyMMddHH")
      val day: Long = TimeUtils.getDate(timestamp, "yyyyMMdd")
      val month: Long = TimeUtils.getDate(timestamp, "yyyyMM")
      // 获取用户的游览信息
      val userScan: UserScan = message.userScan
      val channelID: String = userScan.channelID
      val userID: String = userScan.userID
      // 国家
      val country: String = userScan.country
      // 省份
      val province: String = userScan.province
      // 城市
      val city: String = userScan.city
      // 拼接地域
      val region = country + "-" + province + "-" + city
      // 判断该用户是否是新老用户
      val userState: UserState = UserState.getUserState(userID, timestamp)
      val isNew: Boolean = userState.isNew
      val isFirstHour: Boolean = userState.isFirstHour
      val isFirstDay: Boolean = userState.isFirstDay
      val isFirstMonth: Boolean = userState.isFirstMonth
      // 构建存储ChannelRegion实体对象的数组
      val arrayBuffer: ArrayBuffer[ChannelRegion] = ArrayBuffer[ChannelRegion]()
      // 新用户
      if (isNew) {
        arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 1, 1, 0, timestamp, hour.toString, hour.toString + channelID + region)
        arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 1, 1, 0, timestamp, day.toString, day.toString + channelID + region)
        arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 1, 1, 0, timestamp, month.toString, month.toString + channelID + region)
      } else {
        // 小时
        isFirstHour match {
          case true => arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 1, 0, 1, timestamp, hour.toString, hour.toString + channelID + region)
          case _ => arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 0, 0, 0, timestamp, hour.toString, hour.toString + channelID + region)
        }
        // 天
        isFirstDay match {
          case true => arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 1, 0, 1, timestamp, day.toString, day.toString + channelID + region)
          case _ => arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 0, 0, 0, timestamp, day.toString, day.toString + channelID + region)
        }
        // 月
        isFirstMonth match {
          case true => arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 1, 0, 1, timestamp, month.toString, month.toString + channelID + region)
          case _ => arrayBuffer += ChannelRegion(channelID, country, province, city, 1, 0, 0, 0, timestamp, month.toString, month.toString + channelID + region)
        }
      }
      arrayBuffer
    })
    // 2、分流
    val keyByStream: KeyedStream[ChannelRegion, String] = channelRegionStream.keyBy(x => x.groupField)
    // 3、划分时间窗口
    val timeWindowStream: WindowedStream[ChannelRegion, String, TimeWindow] = keyByStream.timeWindow(Time.seconds(3))
    // 4、对指标聚合  pv  uv  新鲜度
    val result: DataStream[ChannelRegion] = timeWindowStream.reduce((c1, c2) => {
      val channelID: String = c1.channelID
      val timestamp: Long = c1.timestamp
      val country: String = c1.country
      val province: String = c1.province
      val city: String = c1.city
      val dateField: String = c1.dateField
      val groupField: String = c1.groupField
      // 合并pv/uv/newCount/oldCount
      val totalPV = c1.pv + c2.pv
      val totalUV = c1.uv + c2.uv
      val totalNewCount = c1.newCount + c2.newCount
      val totalOldCount = c1.oldCount + c2.oldCount
      ChannelRegion(channelID, country, province, city, totalPV, totalUV, totalNewCount, totalOldCount, timestamp, dateField, groupField)
    })
    // 5、数据落地
    result.addSink(c => {
      // 获取 pv、uv、newCount、oldCount
      var pv: Long = c.pv
      var uv: Long = c.uv
      var newCount: Long = c.newCount
      var oldCount: Long = c.oldCount
      val channelID: String = c.channelID
      val dateField: String = c.dateField
      // 拼接国家省份城市
      val region = c.country + "-" + c.province + "-" + c.city
      // 构建rowkey
      val rowkey = channelID + ":" + dateField + ":" + region
      // 指定TableName、表的列族、表的列
      val tableName: TableName = TableName.valueOf("channel")
      val columnFamily = "info"
      val regionPVColumn = "regionPV"
      val regionUVColumn = "regionUV"
      val regionNewCountColumn = "regionNewCount"
      val regionOldCountColumn = "regionOldCount"
      // 去hbase查询数据，如果有需要累加
      val regionPVData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, regionPVColumn)
      val regionUVData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, regionUVColumn)
      val regionNewCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, regionNewCountColumn)
      val regionOldCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, regionOldCountColumn)
      // 判断
      if (StringUtils.isNotBlank(regionPVData)) {
        pv += regionPVData.toLong
      }
      if (StringUtils.isNotBlank(regionUVData)) {
        uv += regionUVData.toLong
      }
      if (StringUtils.isNotBlank(regionNewCountData)) {
        newCount += regionNewCountData.toLong
      }
      if (StringUtils.isNotBlank(regionOldCountData)) {
        oldCount += regionOldCountData.toLong
      }
      // 封装数据
      var map = Map[String, Long]()
      map += (regionPVColumn -> pv)
      map += (regionUVColumn -> uv)
      map += (regionNewCountColumn -> newCount)
      map += (regionOldCountColumn -> oldCount)
      // 插入数据到hbase表
      HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
    })
  }
}
