package com.lin.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Demo05Capacity {
    public static void main(String[] args) {
        // 创建properties配置
        Properties properties = new Properties();

        // kafka节点
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.200.134:39092");

        // 序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // batch.size：批次大小，默认 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger.ms：等待时间，默认 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator：缓冲区大小，默认 32M：buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // compression.type：压缩，默认 none，可配置值 gzip、snappy、...(压缩一个Batch的数据)
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"gzip");
        // 发送一次request的大小(默认: 1MB)
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,1024*1024*5);//设置为5MB

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
