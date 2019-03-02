package com.andy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-25
 **/
public class JavaStreamingTest {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("streaming");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(5));

        JavaReceiverInputDStream<String> stream = jsc.socketTextStream("node-1", 8888);

        // 拆分每一行数据
        JavaDStream<String> words = stream.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

        // 单词映射成 (word, 1) 的形式
        JavaPairDStream<Object, Integer> pairWords = words.mapToPair((PairFunction<String, Object, Integer>) s -> new Tuple2<>(s, 1));

        // 进行reduce聚合操作
        JavaPairDStream<Object, Integer> result = pairWords.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        // 打印输出结构
        result.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}