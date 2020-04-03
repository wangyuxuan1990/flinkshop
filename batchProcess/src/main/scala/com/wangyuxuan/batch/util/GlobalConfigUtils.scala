package com.wangyuxuan.batch.util

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
  def hbaseZookeeperQuorum = config.getString("hbase.zookeeper.quorum")
}
