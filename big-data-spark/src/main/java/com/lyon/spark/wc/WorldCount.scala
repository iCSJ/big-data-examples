package com.lyon.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wc-spark")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("d:\\tmp\\user.dta")
    val rdd2 = rdd1.flatMap(line => line.split(" "))
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val r = rdd4.collect()
    r.foreach(println)
  }

}
