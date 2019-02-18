package com.andy.spark.sql.spark2

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-23
  **/
object UdaTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    val ranges = spark.range(1, 11)

    ranges.show()


    spark.stop()
  }

}
