package com.harvey.kafka.producer;

import com.harvey.kafka.util.KafkaClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ProducerRunner implements Runnable {
    protected final static Logger logger = LoggerFactory.getLogger(ProducerRunner.class);

    private final Producer<String, String> kafkaProducer;
    private String topic;


    public ProducerRunner(String brokers, String topic) {
        this.topic = topic;
        this.kafkaProducer = new KafkaProducer(KafkaClient.getDefaultKafkaProducerProperties(brokers));
    }


    @Override
    public void run() {
        SimpleDateFormat formatter = new SimpleDateFormat("z yyyy-MM-dd HH:mm:sss");
        logger.info("start sending message to kafka");
        int i = 0;

        while (true) {
            String sendMsg = "Producer message number: " + String.valueOf(++i) + ", created time: " + formatter.format(new Date());
            kafkaProducer.send(new ProducerRecord<>(topic, sendMsg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        logger.error(exception.getLocalizedMessage());
                        exception.printStackTrace();
                    }
                    logger.info("Producer Message: Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                }
            });

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("end sending message to kafka");

            if (i >= 100)
                break;
        }
    }

}
