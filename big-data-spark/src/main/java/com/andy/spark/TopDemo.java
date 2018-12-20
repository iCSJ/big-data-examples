package com.andy.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-20
 **/
public class TopDemo {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("top").setMaster("local"));

        JavaRDD<String> wordsRDD = sc.textFile("D:\\tmp\\user.dta");


        sc.close();

    }

}
