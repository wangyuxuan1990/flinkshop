package com.wangyuxuan.tools

import com.typesafe.config.{Config, ConfigFactory}

/**
 * @author wangyuxuan
 * @date 2020/3/31 10:19 下午
 * @description 获取配置文件的工具类
 */
object GlobalConfigUtils {
  // 默认加载resource目录下的application.conf文件
  private val config: Config = ConfigFactory.load()

  // 封装一些方法来获取配置文件中的参数
  def getBootstrapServers = config.getString("bootstrap.servers")

  def getTopicName = config.getString("topic.name")

  def getGroupId = config.getString("group.id")

  def getAutoCommit = config.getString("enable.auto.commit")

  def getAutoCommitTime = config.getString("auto.commit.interval.ms")

  def getAutoOffsetReset = config.getString("auto.offset.reset")

  def getHbaseZookeeper = config.getString("hbase.zookeeper.quorum")
}
