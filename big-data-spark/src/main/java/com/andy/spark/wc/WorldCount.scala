package com.com.andy.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author Leone
  * @since 2018-06-02
  **/
object WorldCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc-spark").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("d:\\tmp\\user.dta", 2)
    val rdd2 = rdd1.flatMap(_.split(" "))

    //    val rdd3 = rdd2.map(word => {
    //      println("start"); val t = (word, 1); println(t + "end"); t
    //    })


    val rdd3 = rdd2.mapPartitions(it => {
      import scala.collection.mutable.ArrayBuffer
      val buf = ArrayBuffer[String]()
      val tName = Thread.currentThread().getName
      println(tName + "-mapPartitions start")
      for (e <- it) {
        buf.+=("_" + e)
      }
      buf.iterator
    })

    val rdd4 = rdd3.map(word => {
      val tName = Thread.currentThread().getName
      println(tName + "-map " + word)
      (word, 1)
    })

    val rdd5 = rdd4.reduceByKey(_ + _)
    val r = rdd4.collect()
    r.foreach(println)


    //    val rdd4 = rdd3.reduceByKey(_ + _)
    //    val r = rdd4.collect()
    //    r.foreach(println)
  }

}
