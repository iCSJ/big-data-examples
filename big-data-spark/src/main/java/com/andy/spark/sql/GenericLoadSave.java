package com.andy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class GenericLoadSave {

    public static void main(String[] args) {

        // 创建sparkContext
        SparkConf conf = new SparkConf().setAppName("rdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> dataset = sqlContext.read().load("file:///E:\\tmp\\input\\parquet");

        dataset.printSchema();

        dataset.show();






    }

}
