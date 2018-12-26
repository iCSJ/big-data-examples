package com.andy.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-25
 **/
public class TopologyMain {

    private static Logger logger = LoggerFactory.getLogger(CustomerCountBolt.class);

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

        //Storm框架支持多语言，在Java环境下创建一个拓扑，需要使用TopologyBuilder进行构建
        TopologyBuilder builder = new TopologyBuilder();

        //RandomSentenceSpout类，在已知的英文句子中，随机发送一条句子出去
        builder.setSpout("spout1", new CustomerSpout(), 3);

        //SplitSentenceBolt类，主要是将一行一行的文本内容切割成单词
        builder.setBolt("split1", new CustomerSplitBolt(), 9).shuffleGrouping("spout1");

        //WordCountBolt类，对单词出现的次数进行统计
        builder.setBolt("count2", new CustomerCountBolt(), 3).fieldsGrouping("split1", new Fields("word"));

        //启动topology的配置信息
        Config conf = new Config();

        //TOPOLOGY_DEBUG(setDebug),当他被设置成true的话，storm会记录下每个组件所发射的每条消息
        //这在本地环境调试topology很有用。但是在线上这么做的话，会影响性能

        conf.setDebug(false);
        //storm的运行模式有两种：本地模式和分布式模式
        if (Objects.nonNull(args) && args.length > 0) {
            conf.setNumWorkers(3);
            //向集群提交topology
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
        }

    }
}
