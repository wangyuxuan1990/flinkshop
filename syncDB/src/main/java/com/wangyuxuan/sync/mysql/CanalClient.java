package com.wangyuxuan.sync.mysql;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.wangyuxuan.sync.producer.KafkaSend;
import com.wangyuxuan.sync.utils.GlobalConfigUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author wangyuxuan
 * @date 2020/4/3 11:17
 * @description 开启一个canal客户端，同步数据
 */
public class CanalClient {
    public static void main(String[] args) {
        String host = GlobalConfigUtils.host;
        int port = Integer.parseInt(GlobalConfigUtils.port);
        String instance = GlobalConfigUtils.instance;
        String user = GlobalConfigUtils.user;
        String password = GlobalConfigUtils.password;
        CanalConnector conn = getConn(host, port, instance, user, password);
        // 连接上canal之后，开始订阅mysql的binlog日志
        int batchSize = 100;
        // 从1开始循环
        int initCount = 1;
        try {
            conn.connect();
            conn.subscribe(".*\\..*"); // 正则表达式匹配
            conn.rollback();

            int totalCount = 120; // 循环次数

            while (totalCount > initCount) {
                // 每次获取多少数据
                Message message = conn.getWithoutAck(batchSize);
                long id = message.getId();
                int size = message.getEntries().size();
                if (id == -1 || size == 0) {
                    // 没有读取到任何数据
                } else {
                    // 有数据,就解析binlog日志
                    parseBinlog(message.getEntries(), initCount);
                    initCount++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.disconnect();
        }

    }

    /**
     * 解析binlog日志
     *
     * @param entries
     * @param initCount
     */
    private static void parseBinlog(List<CanalEntry.Entry> entries, int initCount) {
        for (CanalEntry.Entry entry : entries) {
            // mysql事务开始前和事务开始后的内容排除掉
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            // 如果不满足就解析binlog日志
            CanalEntry.RowChange rowChange = null;
            try {
                // 获取每一行的改变数据
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
            // 获取关键字段，哪一个数据库有事务发生，哪一张表新增、修改、删除操作
            CanalEntry.EventType eventType = rowChange.getEventType(); // 操作是insert、还是update、还是delete

            // 当前操作的binlog文件名称
            String logfileName = entry.getHeader().getLogfileName();

            // 当前操作的数据在binlog文件的位置
            long logfileOffset = entry.getHeader().getLogfileOffset();

            // 当前操作所属的数据库
            String dbName = entry.getHeader().getSchemaName();

            // 当前操作数据库中的哪一张表
            String tableName = entry.getHeader().getTableName();

            // 解析操作的行数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 判断当前是什么操作
                if (eventType == CanalEntry.EventType.DELETE) {
                    // 删除操作
                    // 获取删除之前的所有列数据
                    dataDetails(rowData.getBeforeColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, initCount);
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    // 新增
                    // 获取新增之后的所有列数据
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, initCount);
                } else if (eventType == CanalEntry.EventType.UPDATE) {
                    // 修改
                    // 获取新增之后的所有列数据
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, initCount);
                }
            }
        }
    }

    /**
     * 解析不用操作的方法
     *
     * @param columns
     * @param logfileName
     * @param logfileOffset
     * @param dbName
     * @param tableName
     * @param eventType
     * @param initCount
     */
    private static void dataDetails(
            List<CanalEntry.Column> columns,
            String logfileName,
            long logfileOffset,
            String dbName,
            String tableName,
            CanalEntry.EventType eventType,
            int initCount) {
        // 找到当前哪些列发送了改变，以及改变的值
        ArrayList<Object> list1 = new ArrayList<Object>();

        for (CanalEntry.Column column : columns) {
            ArrayList<Object> list2 = new ArrayList<Object>();
            list2.add(column.getName());  // 当前发送改变的列的名称
            list2.add(column.getValue()); // 当前发送改变的列的值
            list2.add(column.getUpdated()); // 当前修改、新增、删除等操作是否成功
            list1.add(list2);
        }

        // 发送消息到kafka集群中
        String topic = "binlog";
        String key = UUID.randomUUID().toString();
        // 组装数据
        String data = logfileName + "#CS#" + logfileOffset + "#CS#" + dbName + "#CS#" + tableName + "#CS#" + eventType + "#CS#" + list1 + "#CS#" + initCount;
        // 发送消息
        KafkaSend.sendMessage(topic, key, data);
    }

    /**
     * 构建CanalConnector对象
     *
     * @param host
     * @param port
     * @param instance
     * @param user
     * @param password
     */
    private static CanalConnector getConn(String host, int port, String instance, String user, String password) {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), instance, user, password);
        return connector;
    }
}
