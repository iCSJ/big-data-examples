package com.andy.flink.kafka

import java.util.Properties

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-04
  **/
object FlinkScalaKafka {

  private val ZOOKEEPER_HOST = "node-2:2181,node-3:2181,node-4:2181"

  private val KAFKA_BROKER = "node-2:9092,node-3:9092,node-4:9092"

  private val GROUP_ID = "test-group"

  private val TOPIC_NAME = "flink-topic"

  def main(args: Array[String]): Unit = {

    // get env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint 配置
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // kafka 配置
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", GROUP_ID)

    // 读取 kafka 消息
    val srouce = env.addSource(new FlinkKafkaConsumer010[String](TOPIC_NAME, new SimpleStringSchema(), kafkaProps))

    val result = srouce.flatMap(_.split(" ")
      .filter(_.nonEmpty)
      .map((_, 1)))
      .keyBy(0)
      .sum(1)
      .setParallelism(1)

    result.print()

    env.execute()

  }

}
