package com.andy.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-24
  **/
object KafkaWordCount {

  def main(args: Array[String]): Unit = {

    // offset保存路径
    val checkpointPath = "file:///e:/tmp/spark/streaming/checkpoint/kafka-direct"

    val conf = new SparkConf().setAppName("ScalaKafkaStream").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint(checkpointPath)

    val bootstrapServers = "node-2:9092,node-3:9092,node-4:9092"
    val groupId = "streaming-group"
    val topicName = "streaming-topic"
    val maxPoll = 20000

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val DStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))

    DStream.map(_.value)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .transform(data => {
        data.sortBy(_._2, false)
      }).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
