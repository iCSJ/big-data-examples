package com.andy.spark.sql.spark1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-20
  **/
object DataFrameCreate {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dataFrame").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sparkContext)

    val df = sqlContext.read.json("file:///e:/tmp/input/json")

    df.show()

    df.printSchema()

    df.select("username").show()

    df.select(df.col("username"), df.col("age")).show()

    df.filter(df.col("age") > 18).show()

    df.groupBy("age").count().orderBy("age")

  }

}
