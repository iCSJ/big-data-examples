package com.andy.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-18
  **/
object ScalaTransformationOperation {

  def main(args: Array[String]): Unit = {
    reduceByKey();
  }

  /**
    * map 算子:将原来 RDD 的每个数据项通过 map 中的用户自定义函数 f 映射转变为一个新的元素。源码中 map 算子相当于初始化一个 RDD， 新 RDD 叫做 MappedRDD(this, sc.clean(f))。
    */
  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)

    // 在Spark中创建RDD的创建方式大概可以分为三种：1.从集合中创建RDD；2.从外部存储创建RDD；3.从其他RDD创建。
    val numberRDD = sc.parallelize(numbers, 1)

    //    val multipleNumberRDD = numberRDD.map { num => num * 2 }
    //    val multipleNumberRDD = numberRDD.map(_ * 2)
    val multipleNumberRDD = numberRDD.map(i => i * 2)

    multipleNumberRDD.foreach { num => println(num) }
  }

  /**
    * flatMap 算子: 将原来 RDD 中的每个元素通过函数 f 转换为新的元素，并将生成的 RDD 的每个集合中的元素合并为一个集合，内部创建 FlatMappedRDD(this，sc.clean(f))。
    */
  def flatMap(): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = Array("hello you", "hello me", "hello world")

    val linesRDD = sc.parallelize(lines)

    //    val words: RDD[String] = linesRDD.flatMap {
    //      _.split(" ")
    //    }

    //    linesRDD.flatMap(i => i.split(" "))
    linesRDD.flatMap(_.split(" "))

    linesRDD.foreach(println)
    sc.stop()
  }


  /**
    * filter算子: filter 函数功能是对元素进行过滤，对每个元应用f函数，返回值为 true 的元素 在RDD 中保留，返回值为 false 的元素将被过滤掉。 内部实现相当于生成 FilteredRDD(this，sc.clean(f))。
    */
  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numberPair = sc.parallelize(numbers)

    // 一下三种写法都成立
    //    val result = numberPair.filter(num => num % 2 == 0)
    //    val result = numberPair.filter(_ % 2 == 0)
    val result = numberPair.filter { i => i % 2 == 0 }

    result.foreach(println)
    sc.stop()
  }


  /**
    * groupByKey 算子: 在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD
    */
  def groupByKey(): Unit = {

    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val score = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class3", 60), Tuple2("class2", 79), Tuple2("class1", 85))

    val scoreRDD = sc.parallelize(score, 1)

    val result = scoreRDD.groupByKey()

    result.foreach(println)
    sc.stop()
  }

  /**
    * reduceByKey 算子: 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，与groupByKey类似，reduce任务的个数可以通过第二个可选的参数来设置
    */
  def reduceByKey(): Unit = {
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val score = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class3", 60), Tuple2("class2", 79), Tuple2("class1", 85))

    val scoreRDD = sc.parallelize(score, 1)

    val result = scoreRDD.reduceByKey(_ + _)

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
