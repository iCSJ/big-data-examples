package com.andy.spark.sql.spark2

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object SparkSql2xDemo3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("hdfs://node-1:9000/spark/input5")

    import spark.implicits._

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //    val frame: DataFrame = words.groupBy($"value" as "word").count().sort($"count" desc)
    // 导入聚合函数
    import org.apache.spark.sql.functions._
    val frame: DataFrame = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts").orderBy($"counts" desc)

    frame.show()

    spark.stop()
  }

}
