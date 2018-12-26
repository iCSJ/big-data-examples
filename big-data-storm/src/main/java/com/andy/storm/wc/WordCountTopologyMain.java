package com.andy.storm.wc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class WordCountTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MySpout(), 1);
        topologyBuilder.setBolt("myBolt1", new MySplitBolt(), 10).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("myBolt2", new MyCountBolt()).fieldsGrouping("myBolt1", new Fields("word"));


        Config config = new Config();
        config.setNumWorkers(2);

        StormSubmitter.submitTopology("myWordcount", config, topologyBuilder.createTopology());

//        LocalCluster localCluster = new LocalCluster();


    }

}
