package com.lin.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Demo08Orderliness {
    public static void main(String[] args) {
        // 创建properties配置
        Properties properties = new Properties();

        // kafka节点
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.200.134:39092");

        // 序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 应答级别(all: leader和follower落盘数据了再响应)
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 开启幂等性
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // max.request.size(默认是5)需要小于等于5
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 5);
        // 如果还需要保证数据的唯一型，可能还得开启事务

        // 创建生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            // 异步发送不带回调方法的
            producer.send(new ProducerRecord<>("first", "java-" + (i + 1)));

            // 异步发送带回调方法的
            producer.send(new ProducerRecord<>("first", "java-client-key", "java-callback-" + (i + 1)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                        return;
                    }
                    System.out.println("分区号: " + recordMetadata.partition());
                }
            });
        }

        // 关闭生产者客户端
        producer.close();
    }
}
