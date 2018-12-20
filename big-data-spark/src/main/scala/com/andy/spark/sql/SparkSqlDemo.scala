package com.andy.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object SparkSqlDemo {

  def main(args: Array[String]): Unit = {

    val scSql = SparkSession.builder().master().appName("sparkSql").master("local[*]").getOrCreate()


  }

}
