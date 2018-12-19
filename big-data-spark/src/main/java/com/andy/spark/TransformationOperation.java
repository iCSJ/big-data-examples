package com.andy.spark;

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
 * @since 2018-12-18
 **/
public class TransformationOperation {

    public static void main(String[] args) {
        map();
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
        JavaRDD<Integer> multipleNumberRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sparkContext.close();

    }


}
