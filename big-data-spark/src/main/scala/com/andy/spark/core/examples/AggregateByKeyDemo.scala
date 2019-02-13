package com.andy.spark.core.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-18
  **/
object AggregateByKeyDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("group-test").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("D:\\tmp\\stu.dta")

    val rdd2 = rdd1.flatMap(_.split(" "))

    val rdd3 = rdd2.map((_, 1))


  }

}
