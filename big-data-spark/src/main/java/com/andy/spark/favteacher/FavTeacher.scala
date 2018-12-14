package com.andy.spark.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-08
  **/
object FavTeacher {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\unzipapps\\hadoop-2.7.7")
    val conf = new SparkConf().setAppName("favTeacher").setMaster("local[4]")
    val sc = new SparkContext()
    val lines: RDD[String] = sc.textFile(args(0))


    val teacherAndOne = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      (teacher, 1)
    })
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_ + _)

    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)


    val result = sorted.collect()

    println(result.toBuffer)

    sc.stop()


  }
}
