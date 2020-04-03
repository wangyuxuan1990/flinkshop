package com.wangyuxuan.batch.bean

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * @author wangyuxuan
 * @date 2020/4/3 17:35
 * @description 订单样例类
 */
case class OrderRecord(
                        var orderId: String,
                        var userId: String,
                        var merchantId: String,
                        var orderAmount: Double,
                        var payAmount: Double,
                        var payMethod: String,
                        var payTime: String,
                        var benefitAmount: Double,
                        var voucherAmount: Double,
                        var commodityId: String,
                        var activityNum: String,
                        var createTime: String
                      )

object OrderRecord {
  def apply(json: String): OrderRecord = {
    // {"benefitAmount":"20.0","orderAmount":"300.1","payAmount":"290.0","activityNum":"0","createTime":"2018-11-29 00:00:00","merchantId":"3","orderId":"3","payTime":"2018-11-29 00:00:00","payMethod":"1","voucherAmount":"10.0","commodityId":"1","userId":"3"}
    val jsonObject: JSONObject = JSON.parseObject(json)
    var orderId = ""
    var userId = ""
    var merchantId = ""
    var orderAmount = 0.0
    var payAmount = 0.0
    var payMethod = ""
    var payTime = ""
    var benefitAmount = 0.0
    var voucherAmount = 0.0
    var commodityId = ""
    var activityNum = ""
    var createTime = ""

    benefitAmount = jsonObject.getString("benefitAmount").toDouble
    orderAmount = jsonObject.getString("orderAmount").toDouble
    payAmount = jsonObject.getString("payAmount").toDouble
    activityNum = jsonObject.getString("activityNum")
    createTime = jsonObject.getString("createTime")
    merchantId = jsonObject.getString("merchantId")
    orderId = jsonObject.getString("orderId")
    payTime = jsonObject.getString("payTime")
    payMethod = jsonObject.getString("payMethod")
    voucherAmount = jsonObject.getString("voucherAmount").toDouble
    commodityId = jsonObject.getString("commodityId")
    userId = jsonObject.getString("userId")

    OrderRecord(orderId,
      userId,
      merchantId,
      orderAmount,
      payAmount,
      payMethod,
      payTime,
      benefitAmount,
      voucherAmount,
      commodityId,
      activityNum,
      createTime)
  }

  def main(args: Array[String]): Unit = {
    val json = "{\"benefitAmount\":\"20.0\",\"orderAmount\":\"300.1\",\"payAmount\":\"290.0\",\"activityNum\":\"0\",\"createTime\":\"2018-11-29 00:00:00\",\"merchantId\":\"3\",\"orderId\":\"3\",\"payTime\":\"2018-11-29 00:00:00\",\"payMethod\":\"1\",\"voucherAmount\":\"10.0\",\"commodityId\":\"1\",\"userId\":\"3\"}"

    println(OrderRecord(json))
  }
}