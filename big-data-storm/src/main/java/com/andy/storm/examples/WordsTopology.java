package com.andy.storm.examples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class WordsTopology {


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("word", new TestWordSpout(), 1);
        topologyBuilder.setBolt("exclaim", new ExclamationBolt(), 1).shuffleGrouping("word");
        topologyBuilder.setBolt("print", new PrintBolt(), 1).shuffleGrouping("exclaim");
        Config config = new Config();
        config.setDebug(true);
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopologyWithProgressBar(args[0], config, topologyBuilder.createTopology());
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("test", config, topologyBuilder.createTopology());
            Utils.sleep(30000);
            localCluster.killTopology("test");
            localCluster.shutdown();
        }
    }

}