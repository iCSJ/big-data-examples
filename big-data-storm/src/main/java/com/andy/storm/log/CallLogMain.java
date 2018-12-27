package com.andy.storm.log;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-27
 **/
public class CallLogMain {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new CallLogSpout());
        builder.setBolt("creator-bolt", new CallLogCreatorBolt()).shuffleGrouping("spout");
        builder.setBolt("counter-bolt", new CallLogCounterBolt()).fieldsGrouping("call-log-creator", new Fields("call"));

        LocalCluster localCluster = new LocalCluster();

        Config config = new Config();
        config.setDebug(true);

//        localCluster.submitTopology("logAnalyserStorm", config, builder.createTopology());
//        Thread.sleep(10000);
//        localCluster.shutdown();

        StormSubmitter.submitTopology("top", config, builder.createTopology());
    }

}
