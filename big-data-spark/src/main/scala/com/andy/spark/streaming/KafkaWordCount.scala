package com.andy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-24
  **/
object KafkaWordCount {

  def main(args: Array[String]): Unit = {

    // 设置批次产生的时间间隔
    val ssc = new StreamingContext(new SparkConf().setAppName("kafka-steaming").setMaster("local[2]"), Milliseconds(5000))

    var topic = Map[String, Int]("user" -> 1)
    var groupId = "group-1"
    val brokerList = "node-2:9093,node-3:9092,node-4:9092"

    val zkList = "node-2:2181,node-3:2181,node-4:2181"

    // 创建DStream
//    val lines = KafkaUtils.createDirectStream(ssc, zkList, groupId, topic)




    // 启动spark程序
    ssc.start()
    ssc.awaitTermination()

  }

}
