package com.andy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
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
public class JsonDataSourceLoad {

    public static void main(String[] args) throws AnalysisException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("javaSql");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> json = sqlContext.read().json("file:///e:/tmp/input/json/user.json");

        json.createTempView("t_user");

        Dataset<Row> dataset = sqlContext.sql("select * from t_user where age < 34");

        List<String> collect = dataset.javaRDD().map((Function<Row, String>) row -> row.getString(0)).collect();

        System.out.println(collect);


    }

}
