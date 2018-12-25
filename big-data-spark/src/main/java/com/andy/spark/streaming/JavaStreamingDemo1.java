package com.andy.spark.streaming;

import org.apache.spark.SparkConf;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-25
 **/
public class JavaStreamingDemo1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("streaming");
//        JavaStreamingContext sc = new JavaStreamingContext(conf, Seconds(5));


    }

}
