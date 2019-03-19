package com.andy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-16
 **/
public class JavaDStreamTest {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("javaDStream");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);





    }

}
