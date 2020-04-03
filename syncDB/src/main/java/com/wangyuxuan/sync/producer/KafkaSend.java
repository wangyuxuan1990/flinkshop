package com.wangyuxuan.sync.producer;

import com.wangyuxuan.sync.utils.GlobalConfigUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author wangyuxuan
 * @date 2020/4/3 11:09
 * @description kafka发送消息的业务类
 */
public class KafkaSend {

    /**
     * 生产消息到kafka集群
     *
     * @param topic
     * @param key
     * @param data
     */
    public static void sendMessage(String topic, String key, String data) {
        KafkaProducer<String, String> kafkaProducer = createProducer();
        kafkaProducer.send(new ProducerRecord<String, String>(topic, key, data));
    }

    /**
     * 构建KafkaProducer对象
     *
     * @return
     */
    private static KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfigUtils.servers);
        properties.put(ProducerConfig.RETRIES_CONFIG, GlobalConfigUtils.retries);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, GlobalConfigUtils.bufferMemory);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, GlobalConfigUtils.batchSize);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, GlobalConfigUtils.lingerMs);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GlobalConfigUtils.keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlobalConfigUtils.valueSerializer);
        // 基于配置信息，创建KafkaProducer生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        return kafkaProducer;
    }
}
