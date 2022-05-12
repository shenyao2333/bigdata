package com.sy.bigdata.flink.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @Author: sy
 * @Date: Created by 2022.5.12-23:00
 * @description:
 */
public class TestKafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSourceBuilder<Object> kafkaSource = KafkaSource.builder()
                .setBootstrapServers("kakfaIP").setTopics("test_flink")
                .setGroupId("test").setProperty("auto.offset.reset","latest");

        //env.addSource( kafkaSource)





    }
}
