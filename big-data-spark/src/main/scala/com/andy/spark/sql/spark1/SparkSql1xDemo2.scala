package com.andy.spark.sql.spark1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object SparkSql1xDemo2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sql").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile("hdfs://node-1:9000/spark/input4")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = line(3).toDouble
      println(id, name, age, fv)
      Row(id, name, age, fv)
    })

    val schemal = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    val df = sqlContext.createDataFrame(rowRDD, schemal)
    df.registerTempTable("t_person")
    val frame: DataFrame = sqlContext.sql("select * from t_person order by fv desc, age asc")
    frame.show()
    sc.stop()
  }

}
