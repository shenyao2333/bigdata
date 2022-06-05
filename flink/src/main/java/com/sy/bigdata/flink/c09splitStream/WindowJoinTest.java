package com.sy.bigdata.flink.c09splitStream;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author: sy
 * @Date: Created by 2022.6.5-18:17
 * @description:
 */
public class WindowJoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<User> source1 = env.addSource(new CustomSource());

        DataStreamSource<User> source2 = env.addSource(new CustomSource());


        SingleOutputStreamOperator<User> source1Operator = source1.assignTimestampsAndWatermarks(WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((u, l) -> u.time.getTime()));


        SingleOutputStreamOperator<User> source2Operator = source2.assignTimestampsAndWatermarks(WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((u, l) -> u.time.getTime()));

        source1Operator.join(source2Operator)
                .where(user -> user.name)
                .equalTo(user -> user.name)
                .window( TumblingEventTimeWindows.of(Time.seconds(10)) )
                .apply(new JoinFunction<User, User, String>() {
                    @Override
                    public String join(User user, User user2) throws Exception {

                        return user.name+" 访问了 " +user.url + "  且  "+ user2.name+" ->" + user2.url;
                    }
                }).print();

        env.execute();

    }

}
