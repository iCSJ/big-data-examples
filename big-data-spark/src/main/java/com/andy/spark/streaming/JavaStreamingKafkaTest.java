package com.andy.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * <p>基于Kafka Direct方式实时 word count 程序
 *
 * @author leone
 * @since 2018-12-25
 **/
public class JavaStreamingKafkaTest {

    public static void main(String[] args) throws InterruptedException {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[*]");

        // 创建JavaStreamingContext对象
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        // kafka的brokers
        String brokers = "node-2:9092,node-3:9092,node-4:9092";

        // 创建Kafka参数Map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "g1");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("auto.offset.reset", "latest");

        // 创建Kafka的topics ，里面可以填多个topic
        Collection<String> topics = Collections.singletonList("streaming-topic");

        // 创建DStream
        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        // 拆分Kafka topic里面的数据
        JavaDStream<String> linesSplit = lines.flatMap((FlatMapFunction<ConsumerRecord<Object, Object>, String>) line -> Arrays.asList(line.value().toString().split(" ")).iterator());

        // 单词映射成（word，1）的形式
        JavaPairDStream<String, Integer> word = linesSplit.mapToPair((PairFunction<String, String, Integer>) everyWord -> new Tuple2<>(everyWord, 1));

        // 进行reduce聚合操作
        JavaPairDStream<String, Integer> wordsCount = word.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        // 打印输出结构
        wordsCount.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
