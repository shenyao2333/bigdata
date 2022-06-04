package com.sy.bigdata.flink.c06partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sy
 * @Date: Created by 2022.5.16-20:21
 * @description: 全局分区，即使下游有多个并行子任务，也只会很给一个
 */
public class GlobalTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4);

        streamSource.global().print().setParallelism(4);


        env.execute();


    }
}
