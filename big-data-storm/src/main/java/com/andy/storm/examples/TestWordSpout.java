package com.andy.storm.examples;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class TestWordSpout extends BaseRichSpout {

    private static Logger logger = LoggerFactory.getLogger(TestWordSpout.class);
    private SpoutOutputCollector collector = null;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        Utils.sleep(1000);
        final String[] words = new String[]{"fdfs", "fdfs", "ffsdfs"};
        final Random random = new Random();
        final String word = words[random.nextInt(words.length)];
        collector.emit(new Values(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}