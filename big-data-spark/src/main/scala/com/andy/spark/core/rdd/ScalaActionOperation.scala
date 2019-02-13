package com.andy.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p> scala action 算子
  *
  * @author leone
  * @since 2018-12-19
  **/
object ScalaActionOperation {


  val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)

  def main(args: Array[String]): Unit = {
    sample()
  }

  /**
    * reduce 算子
    */
  def reduce(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val result = rdd.reduce(_ + _)
    println(result)
  }

  /**
    * collect 算子
    */
  def collect(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val map = rdd.map(_ * 2)
    val result = map.collect()
    println(result.foreach(e => println(e)))
    sc.stop()
  }

  /**
    * count 算子
    */
  def count(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val count = rdd.count()
    println(count)
    sc.stop()
  }


  /**
    * first 算子
    */
  def first(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val result = rdd.first()
    println(result)
    sc.stop()
  }


  /**
    * take 算子
    */
  def take(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val top = rdd.take(3)
    for (i <- top) {
      println(i)
    }
    sc.stop()
  }

  /**
    * takeSample 算子
    */
  def takeSample(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val result = rdd.takeSample(true, 15)
    println(result.foreach(e => println(e)))
    sc.stop()
  }

  /**
    * takeOrdered 算子
    */
  def takeOrdered(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val result = rdd.takeOrdered(5)
    println(result.foreach(e => println(e)))
    sc.stop()
  }

  /**
    * top 算子
    */
  def top(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val result = rdd.top(5)
    println(result.foreach(e => println(e)))
    sc.stop()
  }

  /**
    * saveAsTextFile 算子
    */
  def saveAsTextFile(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    // rdd.saveAsTextFile("hdfs://node-1:9000/a.txt")
    rdd.saveAsTextFile("file:///e:/tmp/scalaSaveTextFile")
    sc.stop()
  }

  /**
    * saveAsObjectFile 算子
    */
  def saveAsObjectFile(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    // rdd.saveAsObjectFile("hdfs://node-1:9000/output")
    rdd.saveAsObjectFile("file:///e:/tmp/scalaSaveObjectFile")
    sc.stop()
  }

  /**
    * countByKey 算子
    */
  def countByKey(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(List((3, "a"), (5, "b"), (3, "c"), (2, "a"), (3, "a")))
    val result = rdd.countByKey()
    println(result)
    sc.stop()
  }

  /**
    * countByValue 算子
    */
  def countByValue(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(List(1, 2, 3, 4, 2, 4, 2, 1, 1, 1, 1))
    val result = rdd.countByValue()
    println(result)
    sc.stop()
  }

  /**
    * foreach 算子
    */
  def foreach(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(List(1, 2, 3, 4, 2, 4, 2, 1, 1, 1, 1))
    println(rdd.foreach(e => println(e)))
    sc.stop()
  }

  /**
    * fold 算子
    */
  def fold(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    println(rdd.fold(0)(_ + _))
    sc.stop()
  }

  /**
    * sample 算子
    */
  def sample(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("action").setMaster("local[*]"))
    val rdd = sc.parallelize(numberArray)
    val result = rdd.sample(true, 1)
    println(result.foreach(e => println(e)))
    sc.stop()
  }


}
