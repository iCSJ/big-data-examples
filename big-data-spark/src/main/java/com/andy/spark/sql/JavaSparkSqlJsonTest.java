package com.andy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-10
 **/
public class JavaSparkSqlJsonTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("javaSql");
        JavaSparkContext context = new JavaSparkContext(conf);

        // 创建 sparkSql 上下文
        SparkSession spark = SparkSession.builder()
                .appName("javaSql")
                .config("spark.master", "local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("file:///E:/tmp/spark/data/user.json");
        df.show(3);

        df.createOrReplaceTempView("customer");

        spark.sql("select * from customer where age > 22").show();

//        df.where("age > 23").show();
//        spark.sql("select count(1) from customer").show();

        JavaRDD<Row> javaRDD = df.toJavaRDD();

        // dataFrame 和 RDD 转换
        // javaRDD.foreach(e -> System.out.println(e.getLong(0) + "\t" + e.getLong(1) + "\t" + e.getString(2)));


        df.write().mode(SaveMode.Append).json("file:///E:/tmp/spark/a.json");
//        df.write().json("file:///E:/tmp/spark/output5");

    }

}
