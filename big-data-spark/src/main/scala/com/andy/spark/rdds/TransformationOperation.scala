package com.andy.spark.rdds

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-18
  **/
object TransformationOperation {

  def main(args: Array[String]): Unit = {
    map()
  }

  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)

    val numberRDD = sc.parallelize(numbers, 1)

    val multipleNumberRDD = numberRDD.map { num => num * 2 }

    multipleNumberRDD.foreach { num => println(num) }

  }

}
