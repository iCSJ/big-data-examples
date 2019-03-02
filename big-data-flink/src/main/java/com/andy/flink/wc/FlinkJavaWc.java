package com.andy.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * <p>
 *
 * @author leone
 * @since 2019-02-28
 **/
public class FlinkJavaWc {

    /**
     * 从本地文件读取字符串，按空格分割单词，统计每个分词出现的次数并输出
     */
    public static void main(String[] args) throws Exception {
        // 获取输入参数
        int port;
        String host;
        try {
            ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
            host = params.get("host");
        } catch (Exception e) {
            System.err.println("not param port or host used default node-1:8081");
            port = 8081;
            host = "node-1";
        }

        // get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream(host, port, "\n");

        SingleOutputStreamOperator<WordWithCount> wc = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                String[] words = s.split("\\s");
                for (String str : words) {
                    collector.collect(new WordWithCount(str, 1));
                }
            }
        }).keyBy("words")
                // 指定时间窗口大小
                .timeWindow(Time.seconds(2), Time.seconds(1))
                //.sum("words")
                .reduce((ReduceFunction<WordWithCount>) (t0, t1) -> new WordWithCount(t0.words, t1.count + t0.count));
        // 打印到控制台并设置并行度
        wc.print().setParallelism(2);
        env.execute("word-count");
    }

    public static class WordWithCount {

        private String words;

        private int count;

        public WordWithCount() {
        }

        public WordWithCount(String words, int count) {
            this.words = words;
            this.count = count;
        }

        public String getWords() {
            return words;
        }

        public void setWords(String words) {
            this.words = words;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "words='" + words + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    @Test
    public void javaWordCount() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("node-1", 8082);
        DataStream<Tuple2<String, Integer>> dataStream = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split(" ");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(2), Time.seconds(1)).sum(1);
        dataStream.print();
        env.execute("Java WordCount from SocketTextStream Example");
    }

}
