package com.andy.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-02-28
 **/
public class FlinkJavaWordCount {

    /**
     * 从本地文件读取字符串，按空格分割单词，统计每个分词出现的次数并输出
     */
    public static void main(String[] args) {





    }



}
