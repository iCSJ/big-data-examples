package com.andy.storm.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * <p>
 *
 * @author leone
 * @since 2019-02-21
 **/
public class KafkaTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

       /* String zkHosts = "node-2:2181,node-3:2181,node-4:2181";
        String topicName = "kafka-storm";
        String zkRoot = "/storm";
        String zkSpoutId = UUID.randomUUID().toString();

        BrokerHosts hosts = new ZkHosts(zkHosts);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, zkRoot, zkSpoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();
        Map<String, String> map = new HashMap<>();
        // 配置Kafka broker地址
        map.put("metadata.broker.list", "node-2:9092,node-3:9092,node-4:9092");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "kafka-storm");

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", new SequenceBolt()).shuffleGrouping("spout");
        builder.setBolt("kafka-bolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("kafka");
            cluster.shutdown();
        }*/

        TopologyBuilder builder = new TopologyBuilder();
        String zkConnString = "node-2:2181,node-3:2181,node-4:2181";
        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts,"kafka-storm","/stormKafka",UUID.randomUUID().toString());
        spoutConfig.scheme= new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafkaSpout",kafkaSpout);
        builder.setBolt("split-bolt",new SplitBolt()).shuffleGrouping("kafkaSpout");
        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc",config,builder.createTopology());
    }


}
