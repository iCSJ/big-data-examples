package com.andy.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p> 累加变量
  *
  * @author leone
  * @since 2018-12-20
  **/
object ScalaAccumulatorVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("variable").setMaster("local")
    val sc = new SparkContext(conf)

    val sum = sc.longAccumulator

    val array = Array[Long](1, 2, 3, 4)

    val rdd = sc.parallelize(array)

    rdd.foreach(i => sum.add(i))
    println(sum)
  }


}
