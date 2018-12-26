package com.andy.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class KafkaClient {

    public static void main(String[] args) {

    }

    private final static String TOPIC = "TEST-TOPIC";

    private Properties properties = new Properties();

    @Before
    public void init() {
        properties.put("bootstrap.servers", "node-2:9092,node-3:9092,node-3:9094");
    }

    /**
     * 创建topic
     */
    @Test
    public void createTopic() {
        AdminClient adminClient = AdminClient.create(properties);

        ArrayList<NewTopic> topics = new ArrayList<>();

        NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 1);

        topics.add(newTopic);

        CreateTopicsResult result = adminClient.createTopics(topics);
        try {
            result.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}