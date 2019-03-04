package com.andy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class JavaKafkaProducer {

    private static Logger logger = LoggerFactory.getLogger(JavaKafkaProducer.class);

    private static Producer<String, String> producer;

    private final static String TOPIC = "kafka-test-topic";

    private static final String ZOOKEEPER_HOST = "node-2:2181,node-3:2181,node-4:2181";

    private static final String KAFKA_BROKER = "node-2:9092,node-3:9092,node-4:9092";

    private static Properties properties;

    static {
        properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static void main(String[] args) {

        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 200; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String uuid = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>(TOPIC, Integer.toString(i), uuid));
            logger.info("send message success key: {}, value: {}", i, uuid);
        }
        producer.close();
    }

}
