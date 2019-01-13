package com.andy.spark.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-11
 **/
public class SparkHiveJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        SparkSession spark = SparkSession.builder()
                .appName("hive-java")
                .config("spark.master", "local[*]")
                .enableHiveSupport()
                .getOrCreate();


//        Dataset<Row> ds = spark.sql("create table t_5(id int, name string, age int)row format delimited fields terminated by ','");
        Dataset<Row> ds = spark.sql("select * from t_5 where age > 20 order by age desc");
        ds.show();


    }

}
