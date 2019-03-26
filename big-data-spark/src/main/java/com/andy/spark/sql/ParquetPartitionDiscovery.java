package com.andy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-21
 **/
public class ParquetPartitionDiscovery {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("javaSql");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        Dataset<Row> parquet = sqlContext.read().parquet("file:///e:/tmp/input/parquet/user.parquet");

        parquet.printSchema();

        parquet.show();

    }

}
