package com.andy.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
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
    val sc = new SparkContext(new SparkConf().setAppName("nc-wc-steaming").setMaster("local[2]"))

    // 设置批次产生的时间间隔
    val ssc = new StreamingContext(sc, Seconds(5))

    // 从一个socket端口读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 8888)

    // 对DStream进行操作
    val words: DStream[String] = lines.flatMap(_.split(" "))

    // map操作映射为pairRDD
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))

    // 聚合操作
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    // 打印结果
    reduced.print()

    // 启动spark程序
    ssc.start()
    ssc.awaitTermination()
  }

}
