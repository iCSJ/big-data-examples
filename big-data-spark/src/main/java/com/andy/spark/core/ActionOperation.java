package com.andy.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <p> action 操作
 *
 * @author leone
 * @since 2018-12-19
 **/
public class ActionOperation {


    public static void main(String[] args) {
//        reduce();
//        count();
//        take();
//        countByKey();
        saveAsTextFile();
    }

    /**
     * public static void reduce() {
     * JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action").setMaster("local"));
     * sparkContext.close();
     * }
     */

    public static void reduce() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action").setMaster("local"));
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);
        Integer reduce = numbers.reduce((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
        System.out.println(reduce);
        sparkContext.close();
    }


    public static void collect() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action").setMaster("local"));
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);

        JavaRDD<Integer> map = numbers.map((Function<Integer, Integer>) s -> s * 2);
        List<Integer> collect = map.collect();
        for (Integer num : collect) {
            System.out.println(num);
        }


        sparkContext.close();
    }


    public static void count() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action").setMaster("local"));
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);

        long count = numbers.count();

        System.out.println(count);
        sparkContext.close();
    }

    public static void take() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action").setMaster("local"));
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);

        List<Integer> take = numbers.take(3);
        System.out.println(Arrays.toString(take.toArray()));
        sparkContext.close();
    }

    /**
     * saveAsTextFile 算子
     */
    public static void saveAsTextFile() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action").setMaster("local"));
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);
        numbers.saveAsTextFile("htfs://node-1:9000/tmp.txt");
        sparkContext.close();
    }


    /**
     * countByKey 算子
     */
    public static void countByKey() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action").setMaster("local"));
        List<Tuple2<String, String>> scoresList = Arrays.asList(
                new Tuple2<>("class1", "20"),
                new Tuple2<>("class2", "68"),
                new Tuple2<>("class2", "85"),
                new Tuple2<>("class3", "97"),
                new Tuple2<>("class1", "82")
        );
        JavaPairRDD<String, String> students = sparkContext.parallelizePairs(scoresList);

        Map<String, Long> count = students.countByKey();
        for (Map.Entry<String, Long> stu : count.entrySet()) {
            System.out.println("stu" + stu.getKey() + "val" + stu.getValue());
        }
        sparkContext.close();
    }


}
