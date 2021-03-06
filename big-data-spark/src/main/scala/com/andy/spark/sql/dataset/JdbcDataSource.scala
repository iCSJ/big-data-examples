package com.andy.spark.sql.dataset

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-07
  **/
object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("jdbcDataSource").master("local[*]").getOrCreate()

    val dbTable: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/db01?useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "dbtable" -> "t_user",
        "password" -> "root")
    ).load()

    // 表字段
    dbTable.printSchema()

    // 数据信息
    dbTable.show()


    // 使用函数式编程过滤
    /*val filter: Dataset[Row] = dbTable.filter(e => {
      e.getAs[Int](2) <= 6
    })
    filter.show()*/


    // 使用lambda的方式
//        val r = dbTable.filter($"age" <= 23)
//        r.show()
//        val result: DataFrame = dbTable.select($"id",$"name",$"age" * 10 as "age")
//        val result: DataFrame = dbTable.select($"name")
//        result.show()

    // 写回数据库
    //    val props = new Properties()
    //    props.put("user", "root")
    //    props.put("password", "root")
    //    result.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/spark","t_logs_bak",props)

    //    result.write.text("E:\\tmp\\spark\\text")
    //    result.write.json("E:\\tmp\\spark\\json")
    //    result.write.csv("E:\\tmp\\spark\\csv")
    //    result.write.parquet("E:\\tmp\\spark\\parquet")


    // 读取指定的json文件
    //    val json: DataFrame = sprak.read.json("E:\\tmp\\spark\\json")
    //    val filter: DataFrame = json.where($"age" <= 300)
    //    filter.show()

    //    val csv: DataFrame = sprak.read.csv("E:\\tmp\\spark\\csv")
    //    val pdf: DataFrame = csv.toDF("id", "name", "age")
    //    csv.show()

    //    val parquet: DataFrame = spark.read.parquet("E:\\tmp\\spark\\parquet")
    //    parquet.show()

    spark.close()
  }

}
