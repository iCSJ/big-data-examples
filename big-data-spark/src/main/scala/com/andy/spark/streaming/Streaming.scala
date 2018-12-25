package com.andy.spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, streaming}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-25
  **/
object Streaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streaming").setMaster("local")

    val ss = new StreamingContext(conf, streaming.Duration(5000))

    val stream: ReceiverInputDStream[String] = ss.socketTextStream("", 8081)

    val dStream: DStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    dStream.print()

    ss.start()

    ss.awaitTermination()

    ss.stop()

  }

}
