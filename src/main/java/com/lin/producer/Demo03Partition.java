package com.lin.producer;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class Demo03Partition {
    public static void main(String[] args) {
        // 创建properties配置
        Properties properties = new Properties();

        // kafka节点
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.200.134:39092");

        // 序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.lin.producer.MyPartitioner");

        // 创建生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            // 异步发送不带回调方法的
            producer.send(new ProducerRecord<>("first","java-"+(i+1)));

            // 异步发送带回调方法的
            producer.send(new ProducerRecord<>("first", "java-client-key","java-callback-" + (i + 1)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                        return;
                    }
                    System.out.println("分区号: "+recordMetadata.partition());
                }
            });
        }

        // 关闭生产者客户端
        producer.close();
    }
}
