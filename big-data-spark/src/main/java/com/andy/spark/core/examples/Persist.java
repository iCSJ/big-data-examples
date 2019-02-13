package com.andy.spark.core.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * <p>RDD 持久化
 *
 * @author leone
 * @since 2018-12-19
 **/
public class Persist {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("persist").setMaster("local"));

        JavaRDD<String> stringJavaRDD = sc.textFile("D:\\tmp\\user.dta").cache();
//        JavaRDD<String> stringJavaRDD = sc.textFile("D:\\tmp\\user.dta").persist(StorageLevel.getCachedStorageLevel());
//        JavaRDD<String> stringJavaRDD = sc.textFile("D:\\tmp\\user.dta");
        long begin = System.currentTimeMillis();
        long count = stringJavaRDD.count();
        System.out.println(count);
        long end = System.currentTimeMillis();
        System.out.println("一共花费了:" + (end - begin) + "毫秒！");


        begin = System.currentTimeMillis();
        count = stringJavaRDD.count();
        System.out.println(count);
        end = System.currentTimeMillis();
        System.out.println("一共花费了:" + (end - begin) + "毫秒！");

        sc.close();

    }


}
