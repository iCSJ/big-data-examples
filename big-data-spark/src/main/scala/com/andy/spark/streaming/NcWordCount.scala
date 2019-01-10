package com.andy.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-24
  **/
object NcWordCount {

  def main(args: Array[String]): Unit = {

    // 创建sparkContext
    val sc = new SparkContext(new SparkConf().setAppName("steaming").setMaster("local[2]"))

    // 设置批次产生的时间间隔
    val ssc = new StreamingContext(sc, Milliseconds(5000))

    // 从一个socket端口读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 8888)

    // 对DStream进行操作
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))

    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    // 打印结果
    reduced.print()


    // 启动spark程序

    ssc.start()
    ssc.awaitTermination()

  }

}
