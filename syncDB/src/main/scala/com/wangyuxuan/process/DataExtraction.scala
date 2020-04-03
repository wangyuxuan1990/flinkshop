package com.wangyuxuan.process

import java.util
import java.util.Properties

import com.wangyuxuan.sync.utils.HbaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.TableName

/**
 * @author wangyuxuan
 * @date 2020/4/3 13:45
 * @description 消费kafka数据，接下之后数据落地到hbase表中
 */
object DataExtraction {
  /**
   * 1、指定相关信息（flink对接kafka,hbase）
   * 2、创建流处理环境
   * 3、创建kafka的数据流
   * 4、添加数据源
   * 5、解析kafka中的消息，封装成CanalLog对象
   * 6、数据落地
   */

  // todo:1、指定相关信息
  val zkCluster = "node01:2181,node02:2181,node03:2181"
  val kafkaCluster = "node01:9092,node02:9092,node03:9092"
  val topicName = "binlog"
  val groupId = "consumer-binlog"
  val columnFamily = "info"

  def main(args: Array[String]): Unit = {
    // todo:2、创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo:3、构建kafka数据流
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaCluster)
    properties.setProperty("group.id", groupId)
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topicName, new SimpleStringSchema, properties)
    // todo: 4、添加kafka数据源
    val sourceStream: DataStream[String] = env.addSource(kafkaConsumer)
    // todo: 5、解析kafka中的消息，封装成CanalLog对象
    val binlogStream: DataStream[CanalLog] = sourceStream.map(line => {
      val values: Array[String] = line.split("#CS#")
      val length: Int = values.length
      // String data=logfileName+"#CS#" + logfileOffset + "#CS#" + dbName + "#CS#" + tableName  + "#CS#" + eventType + "#CS#" + list1 + "#CS#" + initCount ;
      val fileName = if (length > 0) values(0) else ""
      val fileOffset = if (length > 1) values(1) else ""
      val dbName = if (length > 2) values(2) else ""
      val tableName = if (length > 3) values(3) else ""
      val eventType = if (length > 4) values(4) else ""
      val columns = if (length > 5) values(5) else ""
      val rowNum = if (length > 6) values(6) else ""

      CanalLog(fileName, fileOffset, dbName, tableName, eventType, columns, rowNum)
    })
    // todo:6、数据落地到hbase
    binlogStream.map(line => {
      // 获取要操作的列
      // line : CanalLog(mysql-bin.000002,666,flink,commodity,INSERT,[[commodityId, 10, true], [commodityName, MacBook Pro, true], [commodityTypeId, 3, true], [originalPrice, 43000.0, true], [activityPrice, 40000.0, true]],1)
      // 更改的列：[[commodityId, 10, true], [commodityName, MacBook Pro, true], [commodityTypeId, 3, true], [originalPrice, 43000.0, true], [activityPrice, 40000.0, true]]
      val columns: String = line.columns
      // 截取需要的信息
      // [commodityId, 10, true], [commodityName, MacBook Pro, true], [commodityTypeId, 3, true], [originalPrice, 43000.0, true], [activityPrice, 40000.0, true]
      val fields: String = substrCanal(columns)
      // 获取操作事件
      val eventType: String = line.eventType
      // 获取变更的列
      val triggerColumns: util.ArrayList[UpdateFields] = getTriggerColumns(fields, eventType)
      // 获取表名
      val tableName: String = line.tableName // 插入hbase的表名与mysql表名保持一致
      // 删除操作
      if (eventType.equals("DELETE")) {
        // 由于购物车表比较特殊这里对表进行划分 ，由于没有特定的主键标识，需要设计对应的rowkey。
        tableName match {
          // 购物车表
          case "shopCartAnalysis" => {
            val rowkey: String = getShopCarAnalysis(fields)
            // 删除表数据
            deleteHbase(tableName, rowkey, columnFamily)
          }
          // 非购物车表
          case _ => {
            // 拼接hbase表的主键
            val rowkey: String = line.dbName + "_" + line.tableName + "_" + getPrimaryKey(fields)
            // 数据库同步操作
            // 删除表数据
            deleteHbase(tableName, rowkey, columnFamily)
          }
        }
      } else {
        // 新增/修改
        if (triggerColumns.size() > 0) {
          tableName match {
            // 购物车表
            case "shopCartAnalysis" => {
              val rowkey: String = getShopCarAnalysis(fields)
              // 数据库同步操作--添加和修改
              insertUpdateHbase(tableName, rowkey, columnFamily, triggerColumns)
            }
            // 非购物车表
            case _ => {
              // 拼接hbase表的主键
              val rowkey: String = line.dbName + "_" + line.tableName + "_" + getPrimaryKey(fields)
              // 数据库同步操作--添加和修改
              insertUpdateHbase(tableName, rowkey, columnFamily, triggerColumns)
            }
          }
        }
      }
    })

    env.execute("DataExtraction")
  }

  /**
   * 删除数据
   *
   * @param tableName
   * @param rowkey
   * @param columnFamily
   */
  def deleteHbase(tableName: String, rowkey: String, columnFamily: String) = {
    HbaseUtils.deleteDataByRowkey(TableName.valueOf(tableName), rowkey, columnFamily)
  }

  /**
   * 更新插入到hbase表中的方法
   *
   * @param tableName
   * @param rowkey
   * @param columnFamily
   * @param triggerColumns
   */
  def insertUpdateHbase(tableName: String, rowkey: String, columnFamily: String, triggerColumns: util.ArrayList[UpdateFields]) = {
    HbaseUtils.putColumnsData(TableName.valueOf(tableName), rowkey, columnFamily, triggerColumns)
  }

  /**
   * 获取购物车分析的rowkey = 添加时间+用户id+产品id+商家id
   *
   * @param fields
   * @return
   */
  def getShopCarAnalysis(fields: String): String = {
    /**
     * ==================购物车分析==================
     * create table shopCartAnalysis(
     * userId int(10)  , 				#用户id
     * commodityId int(10) , 			#产品id
     * commodityNum int(10) , 			#产品数量
     * commodityAmount double(16,2), 	#产品金额
     * addTime datetime,				#添加时间
     * merchantId int(20)				#商家id
     * );
     */
    val array: Array[String] = StringUtils.substringsBetween(fields, "[", "]")

    val userid: String = array(0).split(",")(1).trim
    val commodityId: String = array(1).split(",")(1).trim
    val addTime: String = array(4).split(",")(1).trim
    val merchantId: String = array(5).split(",")(1).trim
    // rowkey = 添加时间+用户id+产品id+商家id
    val rowkey = addTime + userid + commodityId + merchantId

    rowkey
  }

  /**
   * 获取表的主键标识
   *
   * @param fields
   * @return
   */
  def getPrimaryKey(fields: String): String = {
    // [commodityId, 10, true], [commodityName, MacBook Pro, true], [commodityTypeId, 3, true], [originalPrice, 43000.0, true], [activityPrice, 40000.0, true]
    // 获取该记录的commodityId字段的值10
    val array: Array[String] = StringUtils.substringsBetween(fields, "[", "]")
    val primaryKey: String = array(0).split(",")(1)
    primaryKey
  }

  // 获取变更的列
  def getTriggerColumns(columns: String, eventType: String): util.ArrayList[UpdateFields] = {
    // 将字符串转换成数组
    val array: Array[String] = StringUtils.substringsBetween(columns, "[", "]")
    val arrayList: util.ArrayList[UpdateFields] = new util.ArrayList[UpdateFields]()
    eventType match {
      // [commodityId, 6, false], [commodityName, 欧派, false], [commodityTypeId, 3, false], [originalPrice, 10000.0, true], [activityPrice, 40000.0, false]
      case "UPDATE" => {
        for (index <- 0 to array.length - 1) {
          val split: Array[String] = array(index).split(",")
          // 拿到要更新的列
          if (split(2).trim.toBoolean == true) {
            arrayList.add(UpdateFields(split(0), split(1)))
          }
        }
        arrayList
      }
      case "INSERT" => {
        for (index <- 0 to array.length - 1) {
          val split: Array[String] = array(index).split(",")
          // 所有的列都为true
          arrayList.add(UpdateFields(split(0), split(1)))
        }
        arrayList
      }
      // 匹配其他，这里就是DELETE
      case _ => {
        arrayList
      }
    }
  }

  /**
   * 截取字符串操作
   *
   * @param line
   * @return
   */
  def substrCanal(line: String): String = {
    line.substring(1, line.length - 1)
  }
}

// 定义封装binlog日志的样例类
case class CanalLog(
                     fileName: String, //binlog文件名称
                     fileOffset: String, //读取到的文件offset
                     dbName: String, //数据库名称
                     tableName: String, //表名
                     eventType: String, //事件类型
                     columns: String, //当前操作的列名称
                     rowNum: String //当前操作的行数
                   )

// 保存触发变更的列（为true的）   列名：列值
case class UpdateFields(key: String, value: String) //发生改变的key-value