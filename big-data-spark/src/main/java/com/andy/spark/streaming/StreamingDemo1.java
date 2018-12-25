package com.andy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-25
 **/
public class StreamingDemo1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("streaming");
//        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations(), Seconds(5));



    }

}
