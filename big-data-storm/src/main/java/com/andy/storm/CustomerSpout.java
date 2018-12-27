package com.andy.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-25
 **/
public class CustomerSpout extends BaseRichSpout {

    private static Logger logger = LoggerFactory.getLogger(CustomerSpout.class);

    // 用来收集Spout输出的tuple
    private SpoutOutputCollector collector;

    private Random random;

    private String[] sentences = new String[]{
            "the cow jumped over the moon",
            "the dog jumped over the moon",
            "the pig jumped over the gun",
            "the fish jumped over the moon",
            "the duck jumped over the moon",
            "the man jumped over the sun",
            "the girl jumped over the sun",
            "the boy jumped over the sun"};


    // 该方法调用一次，主要由storm框架传入SpoutOutputCollector
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        random = new Random();
        // 连接kafka mysql ,打开本地文件
    }

    /**
     * 将会循环被调用
     * while(true)
     * spout.nextTuple()
     */
    @Override
    public void nextTuple() {
        String sentence = sentences[random.nextInt(sentences.length)];
        collector.emit(new Values(sentence));
        logger.info("customerSpout 发送数据：" + sentence);
    }

    // 消息源可以发射多条消息流stream
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
