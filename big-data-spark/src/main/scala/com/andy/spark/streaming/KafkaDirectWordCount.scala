package com.andy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext,Seconds}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-24
  **/
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("kafka-steaming").setMaster("local[2]")
    // 设置批次产生的时间间隔
    val ssc = new StreamingContext(conf = conf, Seconds(15))

    val topic = Map[String, Int]("wc-topic" -> 1)
    val groupId = "group-1"
    val zkList = "node-2:2181,node-3:2181,node-4:2181"

    // 创建DStream
    val data: DStream[String] = KafkaUtils.createStream(ssc, zkList, groupId, topic).map(_._2)

    val words = data.flatMap(_.split(" "))

    val counts = words.map((_, 1L)).reduceByKey(_ + _)

    counts.saveAsTextFiles("file:///e:/tmp/spark/output")

    counts.print()

    // 启动spark程序
    ssc.start()
    ssc.awaitTermination()
  }

}
