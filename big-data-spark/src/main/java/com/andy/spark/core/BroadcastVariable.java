package com.andy.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * <p> 广播变量
 *
 * @author leone
 * @since 2018-12-20
 **/
public class BroadcastVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("variable").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        final int factor = 3;
        Broadcast<Integer> broadcast = sc.broadcast(factor);

        JavaRDD<Integer> map = numbersRDD.map((Function<Integer, Integer>) s -> s * broadcast.value());

        map.foreach((VoidFunction<Integer>) f -> System.out.println(f));


    }


}
