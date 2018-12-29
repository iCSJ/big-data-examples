package com.andy.storm.wc.group;

import com.andy.storm.util.CommonUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class WcGroupSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private TopologyContext topologyContext;

    private Random random = new Random();

    private String[] words = new String[]{
            "the dog jumped over the moon",
            "the pig jumped over the gun",
            "the fish jumped over the moon",
            "the girl jumped over the sun",
            "the boy jumped over the sun"};

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        CommonUtil.sendToClient(this, "open()", 7777);
        this.collector = spoutOutputCollector;
        this.topologyContext = topologyContext;
    }

    /**
     * 被循环调用
     */
    @Override
    public void nextTuple() {
        CommonUtil.sendToClient(this, "nextTuple()", 8888);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(words[random.nextInt(words.length)]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("words"));
    }


}
