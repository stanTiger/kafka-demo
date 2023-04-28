package com.stan.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomerProducerCallback {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.91:9092,192.168.1.92:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 创建 producer 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 发送消息
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<String, String>("test", "hello kafka " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 没有异常，输出信息到控制台
                    if (exception == null) {
                        System.out.println("topic : " + metadata.topic() + " -> " + " 分区 " + metadata.partition());
                    }else {
                        exception.printStackTrace();
                    }
                }
            });
            Thread.sleep(2000);
        }


        // 关闭资源
        producer.close();
    }
}
