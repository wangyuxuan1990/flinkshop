package com.wangyuxuan

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangyuxuan.bean.{Message, UserScan}
import com.wangyuxuan.task.{ChannelPVUVTask, ChannelRealHotTask, ChannelRegionTask, ChannelUserFreshnessTask, UserBrowserTask, UserNetWorkTask}
import com.wangyuxuan.tools.GlobalConfigUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author wangyuxuan
 * @date 2020/3/31 10:27 下午
 * @description flink实时业务开发的执行总入口
 */
object FlinkConsumerApp {
  def main(args: Array[String]): Unit = {
    // todo: 1、构建flink实时处理的环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // todo: 2、获取kafka相关配置
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", GlobalConfigUtils.getBootstrapServers)
    properties.setProperty("topic.name", GlobalConfigUtils.getTopicName)
    properties.setProperty("group.id", GlobalConfigUtils.getGroupId)
    properties.setProperty("enable.auto.commit", GlobalConfigUtils.getAutoCommit)
    properties.setProperty("auto.commit.interval.ms", GlobalConfigUtils.getAutoCommitTime)
    properties.setProperty("auto.offset.reset", GlobalConfigUtils.getAutoOffsetReset)
    // todo: 3、构建kafka消费者
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](GlobalConfigUtils.getTopicName, new SimpleStringSchema, properties)
    // 添加source数据源
    val source: DataStream[String] = env.addSource(kafkaConsumer)
    // 解析topic中的数据
    val messageDataStream: DataStream[Message] = source.map(line => {
      val value: JSONObject = JSON.parseObject(line)
      // 获取message
      val content: String = value.getString("content")
      // 获取count
      val count: Int = value.getIntValue("count")
      // 获取timeStamp
      val timestamp: Long = value.getLongValue("timestamp")
      // 解析每一行用户游览信息json串，封装成UserScan对象
      val userScan: UserScan = UserScan.toBean(content)
      // 封装用户访问信息到Message对象中
      Message(userScan, count, timestamp)
    })

    // todo:1、实时频道热点统计
//    ChannelRealHotTask.process(messageDataStream)
    // todo: 2、实时频道的PV/UV统计
//    ChannelPVUVTask.process(messageDataStream)
    // todo: 3、实时频道的新鲜度统计
//    ChannelUserFreshnessTask.process(messageDataStream)
    // todo: 4、实时频道的地域统计
//    ChannelRegionTask.process(messageDataStream)
    // todo: 5、实时用户上网类型统计
//    UserNetWorkTask.process(messageDataStream)
    // todo: 6、实时用户上网类型统计
    UserBrowserTask.process(messageDataStream)

    // 启动flink程序
    env.execute("app")
  }
}
