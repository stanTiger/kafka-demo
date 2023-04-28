package com.stan.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomConsumerHandCommit {

    public static void main(String[] args) {
        // 配置
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.91:9092,192.168.1.92:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置消费者组
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "stan9");
        // 设置自动提交
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        // 创建 consumer 对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // 订阅
        ArrayList<String> topics = new ArrayList<>();
        topics.add("test");
        consumer.subscribe(topics);

        // 消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            // 手动提交 offset
//            consumer.commitAsync();
            consumer.commitSync();
        }
    }
}
