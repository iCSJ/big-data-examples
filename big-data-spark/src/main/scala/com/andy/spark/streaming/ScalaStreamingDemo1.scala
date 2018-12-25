package com.andy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * <p>
  *
  * @author leone
  * @since 2018-12-25
  **/
object ScalaStreamingDemo1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[4]")

    val ss = new StreamingContext(conf, Seconds(5))

    val stream: ReceiverInputDStream[String] = ss.socketTextStream("39.108.125.41", 8081)

    stream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ss.start()

    // 等待计算完成
    ss.awaitTermination()
  }

}
