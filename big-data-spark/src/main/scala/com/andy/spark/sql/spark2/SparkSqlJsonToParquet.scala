package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlJsonToParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("jsonToParquet").master("local[*]").getOrCreate()

    val df = spark.read.format("json").load("hdfs://node-1:9000/spark-2.1.3/input1")

    df.write.format("parquet").save("file:///e:/tmp/spark/output1")

    spark.stop()
  }

}
