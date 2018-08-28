package com.harvey.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartLog4jToKafka {
    protected final static Logger logger = LoggerFactory.getLogger(StartLog4jToKafka.class);

    public static void main(String[] args) {
        logger.info("harvey test log4j to kafka!");
        System.out.println();
        logger.warn("harvey test end!");
//		org.apache.kafka.common.network.Selector.poll
//		kafka.producer.KafkaLog4jAppender
//		KafkaLog4jAppender
//		Logger
    }
}
