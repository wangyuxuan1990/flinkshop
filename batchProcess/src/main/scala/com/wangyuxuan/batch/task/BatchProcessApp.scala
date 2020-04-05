package com.wangyuxuan.batch.task

import com.wangyuxuan.batch.bean.{MerchantCountMoney, OrderRecord, OrderRecordWide}
import com.wangyuxuan.batch.util.{HBaseTableInputFormat, HbaseUtils}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.hadoop.hbase.TableName

/**
 * @author wangyuxuan
 * @date 2020/4/3 17:16
 * @description 离线批处理分析
 */
object BatchProcessApp {
  def main(args: Array[String]): Unit = {
    // todo:1、创建批处理环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // todo:2、读取hbase表数据
    val dataSet: DataSet[tuple.Tuple2[String, String]] = env.createInput(new HBaseTableInputFormat("orderRecord"))
    // todo:3、解析数据转换为样例类
    val dataSetOrderRecord: DataSet[OrderRecord] = dataSet.map(x => OrderRecord(x.f1))
    // todo:4、对数据进行预处理转换为OrderRecordWide样例类
    val preProcessData: DataSet[OrderRecordWide] = PreProcessTask.process(dataSetOrderRecord)
    // todo:5、使用flatMap操作生成不同维度的数据
    val merchantCountMoneyData: DataSet[MerchantCountMoney] = preProcessData.flatMap(x => {
      List(
        MerchantCountMoney(x.merchantId, 1, x.payAmount, x.day),
        MerchantCountMoney(x.merchantId, 1, x.payAmount, x.month),
        MerchantCountMoney(x.merchantId, 1, x.payAmount, x.year)
      )
    })
    // todo:6、使用groupBy按照商家ID和日期进行分组
    val groupByData: GroupedDataSet[MerchantCountMoney] = merchantCountMoneyData.groupBy(x => x.merchantId + x.time)
    // todo:7、使用reduceGroup进行聚合计算
    val reducedDataSet: DataSet[MerchantCountMoney] = groupByData.reduceGroup(iter => {
      iter.reduce((x, y) => {
        MerchantCountMoney(x.merchantId, x.orderNum + y.orderNum, x.payAmount + y.payAmount, x.time)
      })
    })
    // todo: 8、将使用collect收集计算结果，并转换为List
    val result: Seq[MerchantCountMoney] = reducedDataSet.collect()
    // todo: 9、使用foreach将数据下沉到HBase的analysis_merchant表中
    result.foreach(merchant => {
      val tableName = "analysis_merchant"
      var rowkey = merchant.merchantId
      if (merchant.time != "-") { // 产品id:时间戳
        rowkey = merchant.merchantId + ":" + merchant.time
      }
      val merchantColName = "merchant"
      val countColName = "count"
      val dateColName = "date"
      val moneyColName = "money"
      val cfName = "info"
      HbaseUtils.putMapData(TableName.valueOf(tableName), rowkey, cfName, Map(
        merchantColName -> merchant.merchantId,
        dateColName -> merchant.time,
        countColName -> merchant.orderNum.toString,
        moneyColName -> merchant.payAmount.toString
      ))
    })
  }
}
