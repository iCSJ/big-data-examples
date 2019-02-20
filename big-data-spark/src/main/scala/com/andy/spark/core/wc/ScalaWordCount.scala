package com.andy.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p> scala world count
  *
  * @author leone
  * @since 2018-12-02
  **/
object ScalaWordCount {

  def main(args: Array[String]): Unit = {

    // 创建spark配置 设置应用程序名称
    val conf = new SparkConf().setAppName("scalaWordCount").setMaster("local[*]")

    // spark 上下文
    val sc = new SparkContext(conf)

    // 指定以后从哪里读取数据创建RDD
    val lines: RDD[String] = sc.textFile(args(0))

    val words = lines.flatMap { line => line.split("\t") }

    val pairs = words.map { word => (word, 1) }

    //    pairs.foreach(w => println(w._1 + "---" + w._2))

    val result = pairs.reduceByKey(_ + _)

    result.foreach(println)

    //    // 切分压片
    //    val words: RDD[String] = lines.flatMap(_.split(" "))
    //
    //    // 将单词和一组合
    //    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //
    //    // 按照key进行聚合
    //    val reduce: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //
    //    // 排序
    //    val sorted: RDD[(String, Int)] = reduce.sortBy(_._2, false)
    //
    //    // 将结果保存到指定位置中
    //    sorted.saveAsTextFile(args(1))

    // 释放资源
    sc.stop()
  }


}
