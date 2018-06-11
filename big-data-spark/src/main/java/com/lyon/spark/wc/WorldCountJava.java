package com.lyon.spark.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: Mr.lyon
 * @CreateBy: 2018-06-10 20:37
 **/
public class WorldCountJava {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("wc-java");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("d:\\tmp\\user.dta");
        
        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                List<String> list = new ArrayList<String>();
                String [] arr = s.split(" ");
                for (String str : arr) {
                    list.add(str);
                }
                return list.iterator();
            }
        });

        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });


        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return null;
            }
        });

        List<Tuple2<String, Integer>> list = rdd4.collect();
        for (Tuple2<String, Integer> t : list) {
            System.out.println(t._1 + ":" + t._2);
        }




    }
}
