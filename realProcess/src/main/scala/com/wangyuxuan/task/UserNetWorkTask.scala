package com.wangyuxuan.task

import com.wangyuxuan.`trait`.DataProcess
import com.wangyuxuan.bean.{Message, UserNetWork, UserScan, UserState}
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
 * @date 2020/4/1 16:36
 * @description 用户游览的上网类型分析
 */
object UserNetWorkTask extends DataProcess {
  override def process(dataStream: DataStream[Message]): Unit = {
    // 1、将消息转换成上网类型UserNetWork实体对象
    val userNetWorkStream: DataStream[UserNetWork] = dataStream.flatMap(message => {
      val timestamp: Long = message.timestamp
      val hour: Long = TimeUtils.getDate(timestamp, "yyyyMMddHH")
      val day: Long = TimeUtils.getDate(timestamp, "yyyyMMdd")
      val month: Long = TimeUtils.getDate(timestamp, "yyyyMM")
      val userScan: UserScan = message.userScan
      // 获取用户上网类型
      val network: String = userScan.network
      // 用户id
      val userID: String = userScan.userID
      // 查询用户的状态
      val userState: UserState = UserState.getUserState(userID, timestamp)
      val isNew: Boolean = userState.isNew
      val isFirstHour: Boolean = userState.isFirstHour
      val isFirstDay: Boolean = userState.isFirstDay
      val isFirstMonth: Boolean = userState.isFirstMonth
      val arrayBuffer: ArrayBuffer[UserNetWork] = ArrayBuffer[UserNetWork]()
      // 判断
      if (isNew) {
        arrayBuffer += UserNetWork(network, 1, 1, 0, timestamp, hour.toString)
        arrayBuffer += UserNetWork(network, 1, 1, 0, timestamp, day.toString)
        arrayBuffer += UserNetWork(network, 1, 1, 0, timestamp, month.toString)
      } else {
        // 小时
        isFirstHour match {
          case true => arrayBuffer += UserNetWork(network, 1, 0, 1, timestamp, hour.toString)
          case _ => arrayBuffer += UserNetWork(network, 1, 0, 0, timestamp, hour.toString)
        }
        // 天
        isFirstDay match {
          case true => arrayBuffer += UserNetWork(network, 1, 0, 1, timestamp, day.toString)
          case _ => arrayBuffer += UserNetWork(network, 1, 0, 0, timestamp, day.toString)
        }
        // 月
        isFirstMonth match {
          case true => arrayBuffer += UserNetWork(network, 1, 0, 1, timestamp, month.toString)
          case _ => arrayBuffer += UserNetWork(network, 1, 0, 0, timestamp, month.toString)
        }
      }
      arrayBuffer
    })
    // 2、分流--按照网络类型分组
    val keyByStream: KeyedStream[UserNetWork, String] = userNetWorkStream.keyBy(x => x.network)
    // 3、划分时间窗口
    val timeWindowStream: WindowedStream[UserNetWork, String, TimeWindow] = keyByStream.timeWindow(Time.seconds(3))
    // 4、将上网类型指标聚合累加
    val reduceDataStream: DataStream[UserNetWork] = timeWindowStream.reduce((u1, u2) => {
      val network: String = u1.network
      val timestamp: Long = u1.timestamp
      val dateField: String = u1.dateField
      val count1: Long = u1.count
      val newCount1: Long = u1.newCount
      val oldCount1: Long = u1.oldCount
      val count2: Long = u2.count
      val newCount2: Long = u2.newCount
      val oldCount2: Long = u2.oldCount
      UserNetWork(network, count1 + count2, newCount1 + newCount2, oldCount1 + oldCount2, timestamp, dateField)
    })
    // 5、结果落地到hbase表中
    reduceDataStream.addSink(line => {
      // 定义hbase表的相关信息
      val tableName: TableName = TableName.valueOf("device")
      val columnFamily = "info"
      val networkCountColumn = "networkCount"
      val networkNewCountColumn = "networkNewCount"
      val networkOldCountColumn = "networkOldCount"
      var count: Long = line.count
      var newCount: Long = line.newCount
      var oldCount: Long = line.oldCount
      val network: String = line.network
      val timestamp: Long = line.timestamp
      // 定义rowkey
      val rowkey: String = TimeUtils.getDate(timestamp, "yyyyMMddHH").toString + ":" + network
      val networkCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, networkCountColumn)
      val networkNewCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, networkNewCountColumn)
      val networkOldCountData: String = HbaseUtils.getData(tableName, rowkey, columnFamily, networkOldCountColumn)
      // 判断
      if (StringUtils.isNotBlank(networkCountData)) {
        count += networkCountData.toLong
      }
      // 判断
      if (StringUtils.isNotBlank(networkNewCountData)) {
        newCount += networkNewCountData.toLong
      }
      // 判断
      if (StringUtils.isNotBlank(networkOldCountData)) {
        oldCount += networkOldCountData.toLong
      }
      var map: Map[String, Long] = Map[String, Long]()
      map += (networkCountColumn -> count)
      map += (networkNewCountColumn -> newCount)
      map += (networkOldCountColumn -> oldCount)
      // 插入数据到hbase表中
      HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
    })
  }
}
