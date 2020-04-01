package com.wangyuxuan.task

import com.wangyuxuan.`trait`.DataProcess
import com.wangyuxuan.bean.{ChannelRealHot, Message}
import com.wangyuxuan.tools.HbaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.hadoop.hbase.TableName

/**
 * @author wangyuxuan
 * @date 2020/4/1 11:59
 * @description 实时频道热点统计
 */
object ChannelRealHotTask extends DataProcess {
  // 重写process方法
  override def process(dataStream: DataStream[Message]): Unit = {
    /**
     * 1、将实时解析出的数据转换成频道热点实体类
     * 2、分流
     * 3、时间窗口划分
     * 4、对频道的点击数进行聚合操作
     * 5、将结果数据进行落地操作
     */
    // 1、将实时解析出的数据转换成频道热点实体类
    val channelDataStream: DataStream[ChannelRealHot] = dataStream.map(message => {
      ChannelRealHot(message.userScan.channelID, message.count)
    })
    // 2、分流
    val keyedStream: KeyedStream[ChannelRealHot, String] = channelDataStream.keyBy(x => x.channelID)
    // 3、时间窗口划分
    val windowStream: WindowedStream[ChannelRealHot, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))
    // 4、对频道的点击数进行聚合操作
    val reduceDataStream: DataStream[ChannelRealHot] = windowStream.reduce((c1, c2) => ChannelRealHot(c1.channelID, c1.count + c2.count))
    // 5、将结果数据进行落地操作
    reduceDataStream.addSink(channelRealHot => {
      // 先去hbase中查询历史数据，如果不为空，就累加合并，如果为空就添加
      val channelID: String = channelRealHot.channelID
      var count: Long = channelRealHot.count
      // 查询数据
      val tableName: TableName = TableName.valueOf("channel")
      val columnFamily: String = "info"
      val column: String = "count"
      val data: String = HbaseUtils.getData(tableName, channelID, columnFamily, column)
      if (StringUtils.isNoneBlank(data)) {
        count += data.toLong
      }
      // 数据封装成map
      val map: Map[String, Long] = Map(column -> count)
      // 数据落地
      HbaseUtils.putMapData(tableName, channelID, columnFamily, map)
    })
  }
}
