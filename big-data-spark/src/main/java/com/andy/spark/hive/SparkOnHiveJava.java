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
public class SparkOnHiveJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        SparkSession spark = SparkSession.builder()
                .appName("hive-java")
                .config("spark.master", "local[*]")
                .enableHiveSupport()
                .getOrCreate();
//        Dataset<Row> ds = spark.sql("create table t_5(id int, name string, age int)row format delimited fields terminated by ','");
//        Dataset<Row> ds = spark.sql("select count(1) from db1.t_app_log");
        Dataset<Row> ds = spark.sql("select sdk_ver,time_zone,language,mac,city,user_id,day from db1.t_app_log order by time_zone limit 200");
        ds.show();
    }

}
