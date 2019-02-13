package com.andy.spark.core.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-18
  **/
object GroupByKeyDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("group-test").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("D:\\tmp\\stu.dta")

    val rdd2: RDD[(String, String)] = rdd1.map(line => {
      val key = line.split(" ")(2)
      (key, line)
    })

    val rdd3 = rdd2.groupByKey()

    rdd3.collect().foreach(t => {
      val key = t._1
      println(key + "-----------")
      for (e <- t._2) {
        println(e)
      }
    })


  }

}
