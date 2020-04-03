package com.wangyuxuan.batch.task

import com.wangyuxuan.batch.bean.OrderRecord
import com.wangyuxuan.batch.util.HBaseTableInputFormat
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

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
    dataSetOrderRecord.print()
  }
}
