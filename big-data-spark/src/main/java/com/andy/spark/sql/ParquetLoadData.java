package com.andy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-21
 **/
public class ParquetLoadData {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("javaSql");
        JavaSparkContext context = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(context);

        Dataset<Row> parquet = sqlContext.read().parquet("file:///e:/tmp/input/parquet/user.parquet");

        parquet.registerTempTable("t_user");

        sqlContext.sql("select * from t_user where age < 34").show();

        JavaRDD<Row> rowJavaRDD = parquet.javaRDD();

        JavaRDD<String> map = rowJavaRDD.map((Function<Row, String>) row -> "name" + row.getString(1));

        List<String> collect = map.collect();
        System.out.println(collect);


    }

}
