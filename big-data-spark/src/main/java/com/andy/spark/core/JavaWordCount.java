package com.andy.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * java版本 word count 统计单词出现次数
 *
 * @author leone
 * @since 2018-06-10
 **/
public class JavaWordCount {

    public static void main(String[] args) {

        // 创建spark配置
        SparkConf sparkConf = new SparkConf().setAppName("word-count").setMaster("local[4]");

        // 创建sparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 创建 初始化RDD lines 每一行元素是一行文本
        JavaRDD<String> lines = sparkContext.textFile("D:\\tmp\\user.dta");

        // 对lines RDD执行mapToPair算子，将每一行映射(key,1)这种类型
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });


        JavaPairRDD<String, Integer> lineCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + "appears " + t._2 + " times.");
            }
        });

        // 关闭sparkContext
        sparkContext.close();

    }

}
