package com.wangyuxuan.bean

/**
 * @author wangyuxuan
 * @date 2020/4/1 16:22
 * @description 定义频道实时地域的样例类
 */
case class ChannelRegion(
                          var channelID: String, //频道id
                          var country: String, //国家
                          var province: String, //省份
                          var city: String, //
                          var pv: Long, //pv
                          var uv: Long, //uv
                          var newCount: Long, //新用户
                          var oldCount: Long, //老用户
                          var timestamp: Long, //时间戳
                          var dateField: String, //时间维度字段
                          var groupField: String //分组的字段
                        )
