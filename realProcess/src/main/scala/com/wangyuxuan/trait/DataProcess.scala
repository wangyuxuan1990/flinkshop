package com.wangyuxuan.`trait`

import com.wangyuxuan.bean.Message
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author wangyuxuan
 * @date 2020/4/1 11:52
 * @description 数据处理的接口
 *              规范代码开发，便于统一管理
 */
trait DataProcess {
  def process(dataStream: DataStream[Message])
}
