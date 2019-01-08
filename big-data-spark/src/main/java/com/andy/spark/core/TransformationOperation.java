package com.andy.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-18
 **/
public class TransformationOperation {

    public static void main(String[] args) {
//        map();
//        filter();
//        flatMap();
//        groupByKey();
//        reduceByKey();
//        sortByKey();
        joinAndCogroup();
    }

    /**
     * map算子：将集合中的每一个元素都乘以二
     */
    public static void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 构造集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        // 并行化集合，初始化RDD
        JavaRDD<Integer> numbersRDD = sparkContext.parallelize(numbers);

        // map 算子是对任何 RDD 都可以调用的，在 java 中 map 算子接受的是 function 对象
        JavaRDD<Integer> multipleNumberRDD = numbersRDD.map((Function<Integer, Integer>) integer -> integer * 2);

        multipleNumberRDD.foreach((VoidFunction<Integer>) integer -> System.out.println(integer));

        sparkContext.close();

    }


    /**
     * filter 算子操作
     */
    public static void filter() {
        SparkConf sparkConf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> paraNumber = sc.parallelize(numbers);

        JavaRDD<Integer> filter = paraNumber.filter((Function<Integer, Boolean>) integer -> integer % 2 == 0);

        filter.foreach((VoidFunction<Integer>) System.out::println);

        sc.close();

    }

    /**
     * flatMap算子：将多行文本拆分成多个单词
     */
    public static void flatMap() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("flatMap").setMaster("local"));

        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");

        JavaRDD<String> lines = sc.parallelize(lineList);

        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

        words.foreach((VoidFunction<String>) s -> System.out.println(s));

        sc.close();
    }

    /**
     * groupByKey算子：分组操作
     */
    public static void groupByKey() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("groupByKey").setMaster("local"));

        List<Tuple2<String, Integer>> scoresList = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 68),
                new Tuple2<>("class2", 85),
                new Tuple2<>("class3", 97),
                new Tuple2<>("class1", 82)
        );
        // 集合并行化
        JavaPairRDD<String, Integer> score = sc.parallelizePairs(scoresList);

        JavaPairRDD<String, Iterable<Integer>> groupScore = score.groupByKey();

        groupScore.foreach((VoidFunction<Tuple2<String, Iterable<Integer>>>) s -> System.out.println(s));

        sc.close();
    }


    /**
     * reduceByKey算子：统计
     */
    public static void reduceByKey() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("reduceByKey").setMaster("local"));

        List<Tuple2<String, Integer>> scoresList = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 68),
                new Tuple2<>("class2", 85),
                new Tuple2<>("class3", 97),
                new Tuple2<>("class1", 82)
        );
        // 集合并行化
        JavaPairRDD<String, Integer> score = sc.parallelizePairs(scoresList);

        JavaPairRDD<String, Integer> pairRDD = score.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) t -> System.out.println(t._1 + "===sum:" + t._2));

        sc.close();
    }


    /**
     * sortByKey 算子：排序
     */
    public static void sortByKey() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sortByKey").setMaster("local"));
        List<Tuple2<Integer, String>> scoresList = Arrays.asList(
                new Tuple2<>(90, "tom"),
                new Tuple2<>(68, "jack"),
                new Tuple2<>(85, "james"),
                new Tuple2<>(82, "andy")
        );
        // 集合并行化
        JavaPairRDD<Integer, String> score = sc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, String> pairRDD = score.sortByKey(false);

        pairRDD.foreach((VoidFunction<Tuple2<Integer, String>>) t -> System.out.println(t._1 + ":" + t._2));

        sc.close();
    }

    /**
     * join cogroup 算子：排序
     */
    public static void joinAndCogroup() {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("joinAndCogroup").setMaster("local"));

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<>(1, "tom"),
                new Tuple2<>(2, "jack"),
                new Tuple2<>(3, "james"),
                new Tuple2<>(4, "andy")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 89),
                new Tuple2<>(4, 97)
        );

        // 并行化两个rdd
        JavaPairRDD<Integer, String> student = sc.parallelizePairs(studentList);

        JavaPairRDD<Integer, Integer> score = sc.parallelizePairs(scoreList);

        JavaPairRDD join = student.join(score);

        join.foreach((VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>) t -> System.out.println("id:" + t._1 + "\t\tname:" + t._2._1 + "\t\tscore:" + t._2._2));

        sc.close();
    }


}
