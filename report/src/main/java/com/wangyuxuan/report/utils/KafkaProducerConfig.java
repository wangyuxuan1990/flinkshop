package com.wangyuxuan.report.utils;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangyuxuan
 * @date 2020/3/29 3:40 下午
 * @description 编写kafka配置工具类
 */
@Configuration
@EnableKafka
public class KafkaProducerConfig {
    @Value("${kafka.producer.servers}")
    private String servers;

    @Value("${kafka.producer.retries}")
    private int retries;

    @Value("${kafka.producer.buffer.memory}")
    private int bufferMemory;

    @Value("${kafka.producer.batch.size}")
    private int batchSize;

    @Value("${kafka.producer.linger.ms}")
    private int lingerMs;

    @Value("${key.serializer}")
    private String keySerializer;

    @Value("${value.serializer}")
    private String valueSerializer;

    // 获取配置信息封装到map中
    public Map<String, Object> getProducerConfig() {
        HashMap<String, Object> hmap = new HashMap<>();
        hmap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        hmap.put(ProducerConfig.RETRIES_CONFIG, retries);
        hmap.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        hmap.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        hmap.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        hmap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        hmap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return hmap;
    }

    // 对SpringBoot提供服务的接口
    // 注册Bean到Spring IoC
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(getProducerConfig()));
    }
}
