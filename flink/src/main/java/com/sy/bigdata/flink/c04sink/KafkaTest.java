package com.sy.bigdata.flink.c04sink;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author: sy
 * @Date: Created by 2022.5.17-09:42
 * @description:
 */
public class KafkaTest {


    public static void main(String[] args) throws Exception {
        DataStreamSource<User> env = CommonParam.getEnv();


        // kafka生产者属性
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put(ProducerConfig.ACKS_CONFIG, 1);
        // 发生错误重试次数
        kafkaProducerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 当有多个消息需要被发送到同一个分区时，生产者会把它们放在同一个批次里。
        // 该参数指定了一个批次可以使用的内存大小，按照字节数计算。
        kafkaProducerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);


        KafkaSink.<String>builder()
                .setKafkaProducerConfig(kafkaProducerProps)
                .setBootstrapServers("192.168.222.140:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("point_topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
        ;


        env.getExecutionEnvironment().execute();


    }


}
