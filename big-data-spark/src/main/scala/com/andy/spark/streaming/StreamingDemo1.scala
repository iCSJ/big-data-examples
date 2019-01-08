package com.andy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-24
  **/
object StreamingDemo1 {

  def main(args: Array[String]): Unit = {
    val group = "group1"
    val conf = new SparkConf().setAppName("steaming").setMaster("local")

    val sc = new StreamingContext(conf, Duration(5000))

    val topic = "wc"

    val brokerList = "node-2:9093,node-3:9092,node-4:9092"

    val zkList = "node-2:2181,node-3:2181,node-4:2181"

    val topics: Set[String] = Set(topic)




  }

}
