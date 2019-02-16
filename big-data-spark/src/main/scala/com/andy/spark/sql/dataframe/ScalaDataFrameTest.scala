package com.andy.spark.sql.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-10
  **/
object DataFrameTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("sparkSql").master("local[*]").getOrCreate()

    val array: Array[String] = Array("1,james,12", "2,jack,23", "3,andy,34")

    val rdd: RDD[String] = sc.makeRDD(array)

    val rdd2 = rdd.map(e => {
      val arr = e.split(",")
      Customer(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val frame = session.createDataFrame(rdd2)
    frame.printSchema()
    frame.show()
    val customer = frame.createTempView("customer")

    val sql = session.sql("select * from customer where age > 20 order by age desc")
    sql.show()

//    frame.selectExpr("id", "name").show()

//    frame.where("age > 20").show()

//    frame.agg(sum("age"),sum("id"))

  }


}

case class Customer(id: Int, name: String, age: Int) {}