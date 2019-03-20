package com.andy.spark.sql.spark1

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-20
  **/
object ScalaRDD2DataFrameProgrammatically {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dataFrame").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sparkContext)

    val rdd = sparkContext.textFile("file:///e:/tmp/input/student")

    val lineRDD = rdd.map { line => {
      val lines = line.split(",")
      Row(lines(0).toInt, lines(1), lines(2).toInt)
    }
    }

    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val dataFrame = sqlContext.createDataFrame(lineRDD, structType)

    dataFrame.createTempView("t_student")
    sqlContext.sql("select * from t_student where age < 23 order by age desc").show()

    val studentRDD = dataFrame.rdd
    studentRDD.foreach(println)


  }

}
