package com.stan.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CustomConsumerForTime {

    public static void main(String[] args) {
        // 配置
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.91:9092,192.168.1.92:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "lina");

        // consumer 对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // 订阅
        ArrayList<String> topics = new ArrayList<>();
        topics.add("test");
        consumer.subscribe(topics);

        // 获取 topicPartitions
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }

        // 填装 topicPartition 时间
        HashMap<TopicPartition, Long> timeForSearch = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            // 封装分区集合存储，每个分区对应一天钱的数据
            timeForSearch.put(topicPartition, System.currentTimeMillis() - 24 * 60 * 60 * 1000);
        }

        // 获取从1天前开始消费的每个分区的 offset
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timeForSearch);

        // consumer 填装每个分区指定时间 offset
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            long offset = offsetAndTimestamp.offset();
            consumer.seek(topicPartition, offset);
        }

        // 开始消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }

    }
}
