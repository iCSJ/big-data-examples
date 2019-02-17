package com.andy.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * <p> spark rdd checkpoint 机制
 *
 * @author leone
 * @since 2019-02-17
 **/
public class JavaSparkRddCheckpoint {

    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {
            SparkConf conf = new SparkConf().setAppName("spark-checkpoint").setMaster("local[*]");
            sc = new JavaSparkContext(conf);

            // 设置 checkpoint 目录
            sc.setCheckpointDir("file:///E:/tmp/spark/output1");

            JavaRDD<String> rdd = sc.textFile("file:///E:/tmp/hadoop/input1");

            // 注意： 现需要对RDD进行缓存
            JavaPairRDD<String, Integer> pairRDD = rdd.flatMapToPair((PairFlatMapFunction<String, String, Integer>) s -> {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                String[] arr = s.split(" ");
                for (String ele : arr) {
                    list.add(new Tuple2<>(ele, 1));
                }
                return list.iterator();
            }).cache();

            //为pairRDD设置检查点
            pairRDD.checkpoint();

            System.err.println("isCheckpointed:" + pairRDD.isCheckpointed() + " ===== checkpoint:" + pairRDD.getCheckpointFile());

            pairRDD.collect();

            System.err.println("isCheckpointed:" + pairRDD.isCheckpointed() + " ===== checkpoint:" + pairRDD.getCheckpointFile());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}