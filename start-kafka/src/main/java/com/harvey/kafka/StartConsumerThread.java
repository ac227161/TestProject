package com.harvey.kafka;

import com.harvey.kafka.consumer.ConsumerThreadPool;
import com.harvey.kafka.producer.ProducerRunner;

public class StartConsumerThread {
    public static final String brokers = "localhost:9092";
    public static final String topic = "my-topic";
    public static final String groupId = "myGroupId01";


    public static void main(String[] args) {
        Thread producerThread = new Thread(new ProducerRunner(brokers, topic));
        //producerThread.start();

        ConsumerThreadPool consumer = new ConsumerThreadPool(brokers, groupId, topic);
        consumer.start();
    }

}
