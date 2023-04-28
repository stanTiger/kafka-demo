package com.stan.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducer {

    public static void main(String[] args) {
        // 连接 kafka 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.91:9092,192.168.1.92:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建 producer 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<String, String>("test", "hello kafka " + i));
        }

        // 关闭资源
        producer.close();
    }
}
