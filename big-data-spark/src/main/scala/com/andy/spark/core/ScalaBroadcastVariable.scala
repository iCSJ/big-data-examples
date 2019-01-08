package com.andy.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object ScalaBroadcastVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("broadcast").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7)
    val numberRDD = sc.parallelize(numbers)

    val broadcast = sc.broadcast(3)

    val map = numberRDD.map(_ * broadcast.value)

    map.foreach(println)
  }

}
