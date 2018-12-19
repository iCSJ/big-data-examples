package com.andy.spark.rdds

import java.util
import java.util.{Arrays, List}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-18
  **/
object TransformationOperation {

  def main(args: Array[String]): Unit = {
    //    map()
    //    filter()
    //    flatMap();
    //    groupByKey();
    //    reduceByKey();

    sortByKey();
  }

  /**
    * map算子
    */
  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)

    val numberRDD = sc.parallelize(numbers, 1)

    val multipleNumberRDD = numberRDD.map { num => num * 2 }

    multipleNumberRDD.foreach { num => println(num) }
  }


  /**
    * filter算子
    */
  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numberPair = sc.parallelize(numbers)
    val result = numberPair.filter(num => num % 2 == 0)
    result.foreach(println)
    sc.stop()
  }

  /**
    * flatMap算子
    */
  def flatMap(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)
    val array = Array("hello you", "hello me", "hello world")

    val lines = sc.parallelize(array)

    val words: RDD[String] = lines.flatMap {
      _.split(" ")
    }
    words.foreach(println)
    sc.stop()
  }


  /**
    * groupByKey 算子
    */
  def groupByKey(): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val array = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class3", 60), Tuple2("class2", 79), Tuple2("class1", 85))

    val score = sc.parallelize(array, 1)

    val result = score.groupByKey()

    result.foreach(println)
    sc.stop()
  }

  /**
    * reduceByKey 算子
    */
  def reduceByKey(): Unit = {
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val array = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class3", 60), Tuple2("class2", 79), Tuple2("class1", 85))

    val score = sc.parallelize(array, 1)

    val result = score.reduceByKey(_ + _)

    result.foreach(println)
    sc.stop()
  }


  /**
    * sortByKey 算子
    */
  def sortByKey(): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val array = Array(Tuple2(79, "jack"), Tuple2(86, "tom"), Tuple2(95, "andy"), Tuple2(90, "james"))

    val score = sc.parallelize(array, 1)

    val result = score.sortByKey(false)

    result.foreach(println)
    sc.stop()
  }


  /**
    * joinAndCogroup 算子
    */
  def joinAndCogroup(): Unit = {
    val conf = new SparkConf().setAppName("joinAndCogroup").setMaster("local")
    val sc = new SparkContext(conf)

    val studentList = Array(new Tuple2[Integer, String](1, "tom"), new Tuple2[Integer, String](2, "jack"), new Tuple2[Integer, String](3, "james"), new Tuple2[Integer, String](4, "andy"))

    val scoreList = Array(new Tuple2[Integer, Integer](1, 100), new Tuple2[Integer, Integer](2, 90), new Tuple2[Integer, Integer](3, 89), new Tuple2[Integer, Integer](4, 97))

    val studentPair = sc.parallelize(studentList)
    val scorePair = sc.parallelize(scoreList)

    val result = studentPair.join(scorePair)

    result.foreach(println)
    sc.stop()
  }




}
