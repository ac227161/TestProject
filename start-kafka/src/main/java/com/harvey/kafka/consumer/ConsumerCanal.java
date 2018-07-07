package com.harvey.kafka.consumer;

import com.alibaba.otter.canal.kafka.client.MessageDeserializer;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.harvey.kafka.util.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerCanal {
    public static final String brokers = "hadoop2:9092,hadoop3:9092,hadoop4:9092";
    public static final String topic = "example";
    public static final String groupId = "canalGroup";

    public static void main(String[] args) throws InvalidProtocolBufferException {
        Properties properties = KafkaClient.getDefaultKafkaConsumerProperties(brokers, groupId, MessageDeserializer.class.getName());
        KafkaConsumer<String, Message> consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList(topic)); // 订阅topic

        while (true) {
            ConsumerRecords<String, Message> consumerRecords = consumer.poll(100);  // 获取topic数据
            for (ConsumerRecord record : consumerRecords) {
                Message message = (Message) record.value();
                if (message.getId() != -1 && message.getEntries().size() > 0) {
                    System.out.println(String.format("===============> start message id :%d", message.getId()));
                    for (CanalEntry.Entry entry : message.getEntries()) {
                        System.out.println(entry.getHeader());
                        System.out.println(entry.getEntryType());
                        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            System.out.println(rowChange);
                        } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                            CanalEntry.TransactionBegin begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                            System.out.println(begin);
                        } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                            CanalEntry.TransactionEnd end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                            System.out.println(end);
                        }

                    }
                }

                System.out.println(message);
            }
        }
    }
}
