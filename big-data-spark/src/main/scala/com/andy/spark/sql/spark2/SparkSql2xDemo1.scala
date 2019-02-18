package com.andy.spark.sql.spark2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object SparkSql2xDemo1 {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("sparkSql").master("local[*]").getOrCreate()

    val lines = session.sparkContext.textFile("hdfs://node-1:9000/spark/input4")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = line(3).toDouble
      Row(id, name, age, fv)
    })

    // 结果类型，表头 用户描述DataFrame
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    val dataFrame: DataFrame = session.createDataFrame(rowRDD, schema)

    import session.implicits._
    val df = dataFrame.where($"fv" > 100).orderBy($"fv" desc, $"age" asc)
    df.show()

    session.stop()
  }

}
