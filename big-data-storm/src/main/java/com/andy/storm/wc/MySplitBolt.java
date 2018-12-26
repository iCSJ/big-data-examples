package com.andy.storm.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class MySplitBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    /**
     * 被循环调用
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        String[] arr = line.split(" ");
        for (int i = 0; i < arr.length; i++) {
            outputCollector.emit(new Values(arr[i], 1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "num"));
    }
}
