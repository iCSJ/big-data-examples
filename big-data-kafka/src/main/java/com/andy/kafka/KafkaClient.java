package com.andy.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Test;

import java.util.*;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class KafkaClient {

    private final static String TOPIC = "kafka-test-topic";

    private static final String ZOOKEEPER_HOST = "node-2:2181,node-3:2181,node-4:2181";

    private static final String KAFKA_BROKER = "node-2:9092,node-3:9092,node-4:9092";

    private static Properties properties = new Properties();

    static {
        properties.put("bootstrap.servers", KAFKA_BROKER);
    }

    /**
     * 创建topic
     */
    @Test
    public void createTopic() {
        AdminClient adminClient = AdminClient.create(properties);
        List<NewTopic> newTopics = Arrays.asList(new NewTopic(TOPIC, 1, (short) 1));
        CreateTopicsResult result = adminClient.createTopics(newTopics);
        try {
            result.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建topic
     */
    @Test
    public void create() {
        ZkUtils zkUtils = ZkUtils.apply(ZOOKEEPER_HOST, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 创建一个3个分区2个副本名为t1的topic
        AdminUtils.createTopic(zkUtils, "t1", 3, 2, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }

    /**
     * 查询topic
     */
    @Test
    public void listTopic() {
        ZkUtils zkUtils = ZkUtils.apply(ZOOKEEPER_HOST, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 获取 topic 所有属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "streaming-topic");

        Iterator it = props.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            System.err.println(entry.getKey() + " = " + entry.getValue());
        }
        zkUtils.close();
    }

    /**
     * 修改topic
     */
    @Test
    public void updateTopic() {
        ZkUtils zkUtils = ZkUtils.apply(ZOOKEEPER_HOST, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "log-test");
        // 增加topic级别属性
        props.put("min.cleanable.dirty.ratio", "0.4");
        // 删除topic级别属性
        props.remove("max.message.bytes");
        // 修改topic 'test'的属性
        AdminUtils.changeTopicConfig(zkUtils, "log-test", props);
        zkUtils.close();

    }

    /**
     * 删除topic 't1'
     */
    @Test
    public void deleteTopic() {
        ZkUtils zkUtils = ZkUtils.apply(ZOOKEEPER_HOST, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.deleteTopic(zkUtils, "t1");
        zkUtils.close();
    }


}