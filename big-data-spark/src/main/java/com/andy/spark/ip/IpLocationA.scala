package com.andy.spark.ip

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-17
  **/
object IpLocationA {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ipLocation").setMaster("local[4]")
    val sc = new SparkContext(conf)

//    val rule: Array[(Long, Long, String)] = MyUtils

  }

}
