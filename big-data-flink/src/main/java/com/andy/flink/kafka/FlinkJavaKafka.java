package com.andy.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-04
 **/
public class FlinkJavaKafka {

    private static final String ZOOKEEPER_HOST = "node-2:2181,node-3:2181,node-4:2181";

    private static final String KAFKA_BROKER = "node-2:9092,node-3:9092,node-4:9092";

    private static final String GROUP = "test-group";

    private static final String TOPIC_NAME = "flink-topic";

    public static void main(String[] args) throws Exception {

        // get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkJavaKafka conn = new FlinkJavaKafka();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP);

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>(TOPIC_NAME, new SimpleStringSchema(), kafkaProps);

        DataStream<String> stream = env.addSource(kafkaConsumer);

        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1).setParallelism(1);

        counts.print();

        env.execute();

    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.split(" ");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}