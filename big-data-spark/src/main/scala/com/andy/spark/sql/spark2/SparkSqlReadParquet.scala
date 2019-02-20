package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("read-parquet").master("local[*]").getOrCreate()

    val dataFrame = spark.read.parquet("file:///E:/tmp/spark/output1")

//    dataFrame.show(10)
    dataFrame.createTempView("t_user")

    spark.sql("select userId,username,age,sex,email,tel,createTime,integral from t_user where age < 30 order by userId asc limit 50").show()


    spark.stop()
  }

}
