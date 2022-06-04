package com.sy.bigdata.flink.c06partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sy
 * @Date: Created by 2022.5.16-20:16
 * @description: 广播分区，会将当前任务分发给下游所有服务
 */
public class BroadcastTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4);

        streamSource.broadcast().print().setParallelism(4);



        env.execute();

    }

}
