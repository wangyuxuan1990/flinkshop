package com.wangyuxuan.bean

/**
 * @author wangyuxuan
 * @date 2020/4/1 15:42
 * @description 定义频道实时的用户新鲜度
 */
case class ChannelUserFreshness(
                                 var channelID: String, //频道id
                                 var newCount: Long, //新用户
                                 var oldCount: Long, //老用户
                                 var timeStamp: Long, // 时间戳
                                 var dataFiled: String, // 时间字段
                                 var groupField: String //按照不同的时间维度+频道分组
                               )
