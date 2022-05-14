package com.sy.bigdata.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author: sy
 * @Date: Created by 2022.5.12-23:00
 * @description:
 */
public class TestKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.222.140:9092")
                .setTopics("test_flink")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        DataStreamSource<String> streamKafkaSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
        KeyedStream<String, Integer> stringIntegerKeyedStream = streamKafkaSource.keyBy(String::hashCode);

        SingleOutputStreamOperator<Integer> map = stringIntegerKeyedStream.map(s -> {
            System.out.println(s);
            return s.hashCode();
        });

        map.print();

        env.execute();


    }
}
