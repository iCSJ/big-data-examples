package com.andy.storm.wc;

import com.andy.storm.util.CommonUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class WcCountBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    private TopologyContext context;

    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        CommonUtil.sendToClient(this, "prepare()", 7777);
        this.outputCollector = outputCollector;
        this.context = topologyContext;
    }

    @Override
    public void execute(Tuple tuple) {
        CommonUtil.sendToClient(this, "execute(" + tuple.toString() + ")", 8888);
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        if (map.containsKey(word)) {
            Integer sum = map.get(word);
            map.put(word, count + sum);
        } else {
            map.put(word, count);
        }
        System.out.println(map);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }


    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + "---" + entry.getValue());
        }

    }
}
