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
    public static void main(String[] args) throws Exception {
        int port;
        String host;

        try {
            ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
            host = params.get("host");
        } catch (Exception e) {
            System.err.println("not param port or host used default node-1:9999");
            port = 9999;
            host = "node-1";
        }


        // get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream(host, port, "\n");

        SingleOutputStreamOperator<WordWithCount> words = text.flatMap((FlatMapFunction<String, WordWithCount>) (s, collector) -> {
            String[] words1 = s.split("\\s");
            for (String str : words1) {
                collector.collect(new WordWithCount(str, 1));
            }
        }).keyBy("words")
                // 指定时间窗口大小
                .timeWindow(Time.seconds(2), Time.seconds(1))
                //.sum("words")
                .reduce((ReduceFunction<WordWithCount>) (t0, t1) -> new WordWithCount(t0.words, t1.count + t0.count));
        // 打印到控制台并设置并行度
        words.print().setParallelism(2);

//        DataStream<Tuple2<String, Long>> dataStream = env
//                // 使用 socket 作为 source
//                .socketTextStream(host, port, "\n")
//                // flatMap DataStream->DataStream 将读入的一份数据，转换成0到n个。这里就是拆分。
//                .flatMap((FlatMapFunction<String, String>) (s, collector) -> {
//                    String[] words = s.split(" ");
//                    for (String word : words) {
//                        collector.collect(word);
//                    }
//                })
//                // map  DataStream->DataStream 读入一份，转换成一份，这里是组装成tuple对
//                .map((MapFunction<String, Tuple2<String, Long>>) s -> new Tuple2<>(s, 1L))
//                // keyBy DataStream->KeyedStream 逻辑上将数据根据key进行分区，保证相同的key分到一起。默认是hash分区
//                .keyBy(0)
//                // window
//                .timeWindow(Time.seconds(2))
//                // sum WindowedStream->DataStream 聚合窗口内容。另外还有min,max等
//                .sum(1);
//        dataStream.print();
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

}
