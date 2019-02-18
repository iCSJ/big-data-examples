package com.andy.spark.sql.spark2

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object SparkSql2xDemo2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("hdfs://node-1:9000/spark/input5")

    import spark.implicits._

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    // 注册表
    val view = words.createTempView("v_wc")

    val resuslt = spark.sql("select value, count(*) as counts from v_wc group by value order by counts desc")

    resuslt.show()

    spark.stop()
  }

}
