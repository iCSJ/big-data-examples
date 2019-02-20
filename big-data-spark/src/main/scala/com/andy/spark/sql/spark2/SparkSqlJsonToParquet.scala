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

    val schema = StructType(Array(
      StructField("userId", IntegerType, false),
      StructField("username", StringType, false),
      StructField("sex", IntegerType, true),
      StructField("age", IntegerType, true),
      StructField("address", StringType, true),
      StructField("tel", StringType, true),
      StructField("email", StringType, true),
      StructField("createTime", DateType, true),
      StructField("integral", IntegerType, true)))

    val df = spark.read.format("json").load("hdfs://node-1:9000/spark-2.1.3/input1")

    df.write.format("parquet").save("file:///e:/tmp/spark/output1")

    // {"userId":317155,"username":"于褥","sex":"女","age":54,"address":"姜沟路12号-11410-5310","tel":"15023490985"
    // ,"email":"tjblqntg@126.com","createTime":"2009-05-21 02:26:22","integral":1086}
    // val dataFrame = df.toDF("userId", "username", "sex", "age", "address", "tel", "email", "createTime", "integral")

    spark.stop()
  }

}
