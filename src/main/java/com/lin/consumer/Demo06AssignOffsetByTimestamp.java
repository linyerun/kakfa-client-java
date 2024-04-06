package com.lin.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Demo06AssignOffsetByTimestamp {
    public static void main(String[] args) {
        // 1.创建消费者的配置对象
        Properties properties = new Properties();
        // 2.给消费者配置对象添加参数(这个ip错误, 它不报错的)
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.200.134:39092");
        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消费者组（组名任意起名）必须(如果这个组名保存在了__consumer_offset队列中, 设置offset也没有效果了)
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-24");

        // 创建消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 注册要消费的主题（可以消费多个主题）
        kafkaConsumer.subscribe(Collections.singletonList("first"));

        //  kafkaConsumer.poll(Duration.ofSeconds(1))执行目的: 得到主题的分区分配, 不然是没办法使用seek的
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            kafkaConsumer.poll(Duration.ofMillis(10));
            assignment = kafkaConsumer.assignment();
        }

        // 需要调用offsetsForTimes, 根据时间戳获取到大于等于时间戳最左边的offset值
        // 准备数据
        HashMap<TopicPartition, Long> timestampToSearch = new HashMap<>();
        long lastTime = System.currentTimeMillis() -  1000;
        for (TopicPartition topicPartition : assignment) {
            timestampToSearch.put(topicPartition, lastTime);
        }
        // 调用
        Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(timestampToSearch);


        // 遍历每个分区，对每个分区设置消费时间。
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            // 根据时间指定开始消费的位置
            if (offsetAndTimestamp != null){
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            }else { // == null, 说明这个值不存在, 定位到最后的offset即可
                Map<TopicPartition, Long> topicPartitionLongMap = kafkaConsumer.endOffsets(Collections.singletonList(topicPartition));
                kafkaConsumer.seek(topicPartition, topicPartitionLongMap.get(topicPartition));
            }
        }

        // 开始消费数据
        // 拉取数据来处理(拉100批就结束了,生产环境可不会)
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < 100; i++) {
            // 设置 1s 中消费一批数据(最大默认有500条数据)
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            // 打印消费到的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.printf("create_time: %s, partition: %d, value: %s\n", simpleDateFormat.format(new Date(consumerRecord.timestamp())), consumerRecord.partition(), consumerRecord.value());
            }
        }

        // 关闭资源
        kafkaConsumer.close();
    }
}
