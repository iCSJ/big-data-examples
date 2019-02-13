package com.andy.spark.core.examples.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p> 统计最受欢迎的老师
  *
  * @author leone
  * @since 2018-12-08
  **/
object FavTeacherSubject {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("favTeacher").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //    val map: RDD[((String, String), Int)] = subjectAndTeacher.map((_, 1))

    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_ + _)

    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }


}
