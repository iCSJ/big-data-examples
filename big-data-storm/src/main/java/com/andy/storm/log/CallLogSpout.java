package com.andy.storm.log;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class CallLogSpout implements IRichSpout {

    // 收集器
    private SpoutOutputCollector collector;

    // 是否完成
    private Boolean completed;

    // 上下文
    private TopologyContext topologyContext;

    private Random random = new Random();

    private Integer index = 0;

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.topologyContext = context;
    }


    @Override
    public void nextTuple() {
        if (this.index < 100) {
            List<String> phoneNumber = new ArrayList<>();
            phoneNumber.add("15890212322");
            phoneNumber.add("15890215523");
            phoneNumber.add("15890215682");
            phoneNumber.add("15890212345");
            Integer localInx = 0;
            while (localInx++ < 100 && this.index++ < 1000) {
                String fromPhoneNumber = phoneNumber.get(random.nextInt(4));
                String toPhoneNumber = phoneNumber.get(random.nextInt(4));
                while (fromPhoneNumber == toPhoneNumber) {
                    toPhoneNumber = phoneNumber.get(random.nextInt(4));
                }

                Integer duration = random.nextInt(4);
                this.collector.emit(new Values(fromPhoneNumber, toPhoneNumber, duration));
            }


        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    /**
     * 声明输出字段
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from", "to", "duration"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
