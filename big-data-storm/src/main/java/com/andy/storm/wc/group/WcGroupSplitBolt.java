package com.andy.storm.wc.group;

import com.andy.storm.util.CommonUtil;
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
public class WcGroupSplitBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    private TopologyContext context;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        CommonUtil.sendToClient(this, "prepare()", 7777);
        this.outputCollector = outputCollector;
        this.context = topologyContext;
    }

    /**
     * 被循环调用
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String[] lines = tuple.toString().split(" ");
        for (int i = 0; i < lines.length; i++) {
            outputCollector.emit(new Values(lines[i], 1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
