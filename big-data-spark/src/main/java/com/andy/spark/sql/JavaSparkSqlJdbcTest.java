package com.andy.spark.sql;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * <p> spark jdbc 查询数据库
 *
 * @author leone
 * @since 2019-01-10
 **/
public class JavaSparkSqlJdbcTest {

    public static void main(String[] args) {

        // 创建 sparkSql 上下文
        SparkSession spark = SparkSession.builder()
                .appName("javaSql").config("spark.master", "local[*]").getOrCreate();

        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/spark")
                .option("dbtable", "t_logs")
                .option("user", "root")
                .option("password", "root")
                .option("driver", "com.mysql.jdbc.Driver")
                .load();

        df.show();

        df.select(new Column("id"), new Column("name")).where("name like 'andy'").distinct().show();

        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "root");
        prop.put("driver", "com.mysql.jdbc.Driver");
        df.write().jdbc("jdbc:mysql://localhost:3306/spark?useSSL=false", "t_logs_1", prop);

        spark.close();

    }

}
