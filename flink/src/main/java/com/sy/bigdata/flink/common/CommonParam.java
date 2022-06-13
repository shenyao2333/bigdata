package com.sy.bigdata.flink.common;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;

/**
 * @Author: sy
 * @Date: Created by 2022.5.14-23:34
 * @description:
 */
public class CommonParam {


    @SneakyThrows
    public static  DataStreamSource<User> getEnv(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        return env.fromElements(
                new User("小明","home",new Date(2342342)),
                new User("小红","index",new Date(344202313)),
                new User("小红","goods",new Date(344242343)),
                new User("小红","cart",new Date(23421143)),
                new User("老王","index",new Date(2342343)),
                new User("老王","home",new Date(24545343)),
                new User("小明","cart",new Date(334564243)),
                new User("老王","goods",new Date(345534234))
        );

    }


    @SneakyThrows
    public static  SingleOutputStreamOperator<User> getCustomEnv(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomSource());

        SingleOutputStreamOperator<User> userSingleOutputStreamOperator = userDataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((user, l) -> user.time.getTime()));

        return  userSingleOutputStreamOperator;
    }


}
