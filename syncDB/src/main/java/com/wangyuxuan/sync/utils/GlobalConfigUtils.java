package com.wangyuxuan.sync.utils;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * @author wangyuxuan
 * @date 2020/4/3 11:00
 * @description 获取配置文件属性工具类
 */
public class GlobalConfigUtils {
    static ResourceBundle bundle = ResourceBundle.getBundle("application", Locale.ENGLISH);
    // 获取host
    public static String host = bundle.getString("canal.host").trim();

    // 获取port
    public static String port = bundle.getString("canal.port").trim();

    // 获取instance
    public static String instance = bundle.getString("canal.instance").trim();

    // 获取user
    public static String user = bundle.getString("mysql.user").trim();

    // 获取password
    public static String password = bundle.getString("mysql.password").trim();

    // kafka集群地址
    public static String servers = bundle.getString("kafka.producer.servers").trim();

    // 重试次数
    public static String retries = bundle.getString("kafka.producer.retries").trim();

    // 缓冲区大小
    public static String bufferMemory = bundle.getString("kafka.producer.buffer.memory").trim();

    // 批量发送大小
    public static String batchSize = bundle.getString("kafka.producer.batch.size").trim();

    // 是否延迟发送
    public static String lingerMs = bundle.getString("kafka.producer.linger.ms").trim();

    // key的序列化
    public static String keySerializer = bundle.getString("key.serializer").trim();

    // value的序列化
    public static String valueSerializer = bundle.getString("value.serializer").trim();

    public static void main(String[] args) {
        System.out.println(host + "----" + port + "---" + instance + "---" + user + "---" + password);
    }
}
