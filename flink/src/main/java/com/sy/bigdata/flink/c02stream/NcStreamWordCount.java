package com.sy.bigdata.flink.c02stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sy
 * @Date: Created by 2022.4.27-15:22
 * @description:
 */
public class NcStreamWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("192.168.222.140", 8889);

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = streamSource.flatMap(new LetterCountFlatMap());

        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = flatMap.keyBy(item -> item.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);


        sum.print();
        env.execute();


    }
}
