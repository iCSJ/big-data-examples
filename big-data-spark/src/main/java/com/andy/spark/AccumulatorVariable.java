package com.andy.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * <p> 累加变量
 *
 * @author leone
 * @since 2018-12-20
 **/
public class AccumulatorVariable {


    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("accumulator").setMaster("local"));

        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");

        Accumulator<Integer> sum = sc.accumulator(0);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);

        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

        numberRDD.foreach((VoidFunction<Integer>) s -> sum.add(s));

        System.out.println(sum.value());

        sc.close();

    }

}
