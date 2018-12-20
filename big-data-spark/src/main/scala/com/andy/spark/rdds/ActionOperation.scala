package com.andy.spark.rdds

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p> scala action 操作
  *
  * @author leone
  * @since 2018-12-19
  **/
object ActionOperation {

  def main(args: Array[String]): Unit = {
    //    reduce()
    //    count()
    take()
  }

  /**
    * reduce 操作
    */
  def reduce(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local"))
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numbers = sc.parallelize(numberArray)
    val result = numbers.reduce(_ + _)
    println(result)
  }

  /**
    * reduce 操作
    */
  def collect(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local"))
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numbers = sc.parallelize(numberArray)
    val map = numbers.map(_ * 2)
    val result = map.collect()
    println(result)
    sc.stop()
  }

  /**
    * count 操作
    */
  def count(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local"))
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numbers = sc.parallelize(numberArray)
    val count = numbers.count()
    println(count)
    sc.stop()
  }

  /**
    * take 操作
    */
  def take(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local"))
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numbers = sc.parallelize(numberArray)
    val top = numbers.take(3)
    for (i <- top) {
      println(i)
    }
    sc.stop()
  }

  /**
    * saveAsFile 操作
    */
  def saveAsTextFile(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local"))
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numbers = sc.parallelize(numberArray)
    numbers.saveAsTextFile("hdfs://node-1:9000/a.txt")
    sc.stop()
  }

  /**
    * countByKey 操作
    */
  def countByKey(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local"))

    sc.stop()
  }

}
