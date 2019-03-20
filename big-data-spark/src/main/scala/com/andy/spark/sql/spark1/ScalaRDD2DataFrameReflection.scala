package com.andy.spark.sql.spark1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-20
  **/
object ScalaRDD2DataFrameReflection {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dataFrame").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sparkContext)

    // 导入隐式转换
    import sqlContext.implicits._

    val rdd = sparkContext.textFile("file:///e:/tmp/input/student")

    val studentRDD = rdd.map { line => line.split(",") }.map { arr => Student(arr(0).trim().toInt, arr(1), arr(2).toInt) }

    val studentDF = studentRDD.toDF()


    val view = studentDF.createTempView("t_student")

    sqlContext.sql("select * from t_student where age < 25 order by age asc").show()

    val studentRDD1 = studentDF.rdd

    val rows = studentRDD1.collect()
    rows.foreach(println)


  }

  case class Student(id: Int, name: String, age: Int)

}
