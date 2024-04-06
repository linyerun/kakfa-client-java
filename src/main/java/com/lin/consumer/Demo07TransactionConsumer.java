package com.lin.consumer;

public class Demo07TransactionConsumer {
    public static void main(String[] args) {
        // Kafka消费端将消费过程和提交offset过程做原子绑定
        // 确保消息消费和offset提交了, 有一个环节有问题需要回滚
        // 因为offset提交回滚不了, 但是我们可以不提交它, 那就可以了实现"回滚了"
    }
}
