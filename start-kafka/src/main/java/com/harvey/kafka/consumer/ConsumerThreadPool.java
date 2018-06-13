package com.harvey.kafka.consumer;

import com.harvey.kafka.util.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerThreadPool {
    protected static final Logger logger = LoggerFactory.getLogger(ConsumerThreadPool.class);

    private ExecutorService executorService;

    private final KafkaConsumer<String, String> consumer;
    private final int threadNumber = 4;

    public ConsumerThreadPool(String brokers, String groupId, String topic) {
        this.consumer = new KafkaConsumer<>(KafkaClient.getDefaultKafkaConsumerProperties(brokers, groupId));
        this.consumer.subscribe(Arrays.asList(topic));
    }

    public static class ConsumerRunner implements Runnable {
        private ConsumerRecord consumerRecord;

        public ConsumerRunner(ConsumerRecord consumerRecord) {
            this.consumerRecord = consumerRecord;
        }

        @Override
        public void run() {
            logger.info("Consumer Message: " + consumerRecord.value() + ", Partition:" + consumerRecord.partition() + ", Offset:" + consumerRecord.offset());
        }
    }

    public void start() {
        executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord item : consumerRecords) {
                executorService.submit(new ConsumerRunner(item));
            }

        }
    }

}
