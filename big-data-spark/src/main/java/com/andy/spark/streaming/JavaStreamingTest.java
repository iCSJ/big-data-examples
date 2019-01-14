package com.andy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
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

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("streaming");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Seconds.apply(5));
        JavaReceiverInputDStream<String> stream = sc.socketTextStream("node-1", 8888);
//        JavaDStream<Object> javaDStream = stream.flatMap((FlatMapFunction<String, Object>) s -> Arrays.asList(s.split(" ")).iterator());
//        javaDStream.mapToPair((PairFunction<String, String, Integer>) o -> new Tuple2<String, Integer>(o, 1));
    }


}