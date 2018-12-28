package com.andy.storm.log;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Objects;

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
        builder.setBolt("counter-bolt", new CallLogCounterBolt()).fieldsGrouping("creator-bolt", new Fields("call"));

        Config config = new Config();
        config.setDebug(true);

        // storm的运行模式有两种：本地模式和分布式模式
        if (Objects.nonNull(args) && args.length > 0) {
            config.setNumWorkers(3);
            // [集群模式] 注意后面一定要跟上任务的名称
            // $STORM_HOME/bin/storm jar xxx.jar com.xxx.xxx.log.TopologyMain call-log
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        } else {
            // [本地模式]
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("call-log", config, builder.createTopology());
            config.setMaxTaskParallelism(3);
            Thread.sleep(10000);
            localCluster.shutdown();
        }

    }

}
