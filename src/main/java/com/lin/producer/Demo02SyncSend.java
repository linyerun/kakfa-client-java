package com.lin.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Demo02SyncSend {
    public static void main(String[] args) {
        // 创建properties配置
        Properties properties = new Properties();

        // kafka节点
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.200.134:39092");

        // 序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        long begin = System.currentTimeMillis();

        // 发送数据
        for (int i = 0; i < 10; i++) {
            try {
                // 同步发送不带回调方法的
                RecordMetadata msgMetaData = producer.send(new ProducerRecord<>("first", "sync-java-" + (i + 1))).get();
                System.out.println(msgMetaData);

                // 同步发送带回调方法的
                msgMetaData = producer.send(new ProducerRecord<>("first", "sync-java-" + (i + 1)), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                            return;
                        }
                        System.out.println(recordMetadata.toString());
                    }
                }).get();
                System.out.println(msgMetaData);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // 532毫秒
        System.out.println(System.currentTimeMillis() - begin);

        // 关闭生产者客户端
        producer.close();
    }
}
