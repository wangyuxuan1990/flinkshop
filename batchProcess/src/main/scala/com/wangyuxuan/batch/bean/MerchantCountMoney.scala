package com.wangyuxuan.batch.bean

/**
 * @author wangyuxuan
 * @date 2020/4/3 18:02
 * @description 样例类MerchantCountMoney
 */
case class MerchantCountMoney(
                               merchantId: String, //商家id
                               orderNum: Int, //订单数
                               payAmount: Double, //支付金额
                               time: String //时间
                             )
