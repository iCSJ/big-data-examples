package com.andy.spark.sql.spark1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-21
  **/
object ParquetLoadData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("parquet").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val usersDF = sqlContext.read.parquet("file:///e:/tmp/input/parquet/user.parquet")

    usersDF.createTempView("t_user")

    val dataFrame = sqlContext.sql("select * from t_user where age < 32")

    dataFrame.show()

    dataFrame.rdd.map { row => "name: " + row(1) }.collect().foreach(print)


  }


}
