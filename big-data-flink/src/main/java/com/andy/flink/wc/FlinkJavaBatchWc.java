package com.andy.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-01
 **/
public class FlinkJavaBatchWc {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("e:/tmp/flink/input");

        DataSet<Tuple2<String, Integer>> sum = dataSource.flatMap(new Tokenizer()).groupBy(0).sum(1);

        sum.writeAsCsv("e://tmp/flink/output", "\n", " ");

        env.execute("batch-wc");

    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toUpperCase().split("\\W");
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }


}
