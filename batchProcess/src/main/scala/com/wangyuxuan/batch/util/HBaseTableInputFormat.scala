package com.wangyuxuan.batch.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.hadoop.hbase.client.{Connection, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}

/**
 * @author wangyuxuan
 * @date 2020/4/3 16:45
 * @description Flink整合HBase
 */
class HBaseTableInputFormat(var tableName: String) extends TableInputFormat[org.apache.flink.api.java.tuple.Tuple2[String, String]] {
  var conn: Connection = _

  override def getScanner: Scan = {
    scan = new Scan()
    scan
  }

  override def getTableName: String = tableName

  override def mapResultToTuple(result: Result): org.apache.flink.api.java.tuple.Tuple2[String, String] = {
    val rowkey: String = Bytes.toString(result.getRow)
    // 获取列单元格
    val cellArray: Array[Cell] = result.rawCells()
    val jsonObject: JSONObject = new JSONObject()
    for (i <- 0 until cellArray.size) {
      val columnName: String = Bytes.toString(CellUtil.cloneQualifier(cellArray(i)))
      val value: String = Bytes.toString(CellUtil.cloneValue(cellArray(i)))
      jsonObject.put(columnName, value)
    }
    new org.apache.flink.api.java.tuple.Tuple2[String, String](rowkey, JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect))
  }

  override def close(): Unit = {
    if (table != null) {
      table.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}
