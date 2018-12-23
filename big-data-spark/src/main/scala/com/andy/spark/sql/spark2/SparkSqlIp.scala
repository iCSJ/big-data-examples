package com.andy.spark.sql.spark2

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-21
  **/
object SparkSqlIp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("hdfs://node-1:9000/spark/input2")

    import spark.implicits._

    // 表一
    val ruleDataFrame = lines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3)
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")


    spark.stop()
  }

}
