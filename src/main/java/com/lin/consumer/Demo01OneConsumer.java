package com.lin.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Demo01OneConsumer {
    public static void main(String[] args) {
        // 1.创建消费者的配置对象
        Properties properties = new Properties();
        // 2.给消费者配置对象添加参数(这个ip错误, 它不报错的)
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.200.134:39092");
        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消费起始offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 配置消费者组（组名任意起名）必须(如果这个组名保存在了__consumer_offset队列中, 设置offset也没有效果了)
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-09");

        // 创建消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 注册要消费的主题（可以消费多个主题）
        kafkaConsumer.subscribe(Collections.singletonList("first"));

        // 拉取数据来处理(拉100批就结束了,生产环境可不会)
        for (int i = 0; i < 100; i++) {
            System.out.println("第" + (i + 1) + "批数据:");
            // 设置 1s 中消费一批数据(最大默认有500条数据)
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            // 打印消费到的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }

        // 关闭资源
        kafkaConsumer.close();
    }
}
