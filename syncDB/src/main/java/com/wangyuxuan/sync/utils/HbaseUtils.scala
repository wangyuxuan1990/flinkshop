package com.wangyuxuan.sync.utils

import java.util

import com.wangyuxuan.process.UpdateFields
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
 * @author wangyuxuan
 * @date 2020/4/3 14:08
 * @description 开发hbase的工具类
 */
object HbaseUtils {
  // 1、获取配置对象
  private val conf: Configuration = HBaseConfiguration.create()
  // 2、设置配置参数
  conf.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181")
  // 3、获取hbase数据库连接
  private val conn: Connection = ConnectionFactory.createConnection(conf)
  // 4、获取admin对象
  private val admin: Admin = conn.getAdmin

  /**
   * 创建表的方法
   *
   * @param tableName
   * @param columnFamily
   * @return
   */
  def createTable(tableName: TableName, columnFamily: String): Table = {
    // 1、构建表的描述器
    val tableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)
    // 2、构建列族描述器
    val columnDescriptor: HColumnDescriptor = new HColumnDescriptor(columnFamily)
    // 关联表描述器与列族描述器
    tableDescriptor.addFamily(columnDescriptor)
    // 3、表不存在就创建表
    this.synchronized {
      if (!admin.tableExists(tableName)) {
        admin.createTable(tableDescriptor)
      }
    }
    conn.getTable(tableName)
  }

  /**
   * 添加多个字段到hbase表中
   *
   * @param tableName
   * @param rowkey
   * @param columnFamily
   * @param triggerColumns
   */
  def putColumnsData(tableName: TableName, rowkey: String, columnFamily: String, triggerColumns: util.ArrayList[UpdateFields]) = {
    val table: Table = createTable(tableName, columnFamily)
    try {
      val put: Put = new Put(rowkey.getBytes)
      val puts: util.ArrayList[Put] = new util.ArrayList[Put]()
      for (index <- 0 to triggerColumns.size() - 1) {
        val updateFields: UpdateFields = triggerColumns.get(index)
        // 列名
        val filedName: String = updateFields.key
        // 列值
        val value: String = updateFields.value
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(filedName), Bytes.toBytes(value))
        puts.add(put)
      }
      // 提交
      table.put(puts)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * 按照rowkey去删除数据
   *
   * @param tableName
   * @param rowkey
   * @param columnFamily
   */
  def deleteDataByRowkey(tableName: TableName, rowkey: String, columnFamily: String) = {
    val table: Table = createTable(tableName, columnFamily)
    try {
      // 基于rowkey构建delete对象
      val delete: Delete = new Delete(rowkey.getBytes)
      // 删除
      table.delete(delete)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }
}
