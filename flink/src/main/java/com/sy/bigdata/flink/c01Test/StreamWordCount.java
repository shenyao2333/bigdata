package com.sy.bigdata.flink.c01Test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sy
 * @Date: Created by 2022.4.26-14:54
 * @description:
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\item\\bigdata\\flink\\src\\main\\resources\\document.txt");

        //转化计算
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple = dataStreamSource
                .flatMap(new WordCountFlatMap());

       // 分组
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream =
                tuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);

        //打印
        sum.print();

        // 启动执行
        env.execute();

    }

}
