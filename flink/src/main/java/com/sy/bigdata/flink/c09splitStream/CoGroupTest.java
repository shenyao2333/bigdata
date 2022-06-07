package com.sy.bigdata.flink.c09splitStream;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: sy
 * @Date: Created by 2022.6.7-22:40
 * @description:
 */
public class CoGroupTest {

    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<User> source1 = env.addSource(new CustomSource());

        DataStreamSource<User> source2 = env.addSource(new CustomSource());


        SingleOutputStreamOperator<User> source1Operator = source1.assignTimestampsAndWatermarks(WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((u, l) -> u.time.getTime()));


        SingleOutputStreamOperator<User> source2Operator = source2.assignTimestampsAndWatermarks(WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((u, l) -> u.time.getTime()));

        source1Operator.coGroup(source2Operator)
                .where(user -> user.name)
                .equalTo(user -> user.name)
                .window( TumblingEventTimeWindows.of(Time.seconds(5)) )
                .apply(new CoGroupFunction<User, User, String>() {
                    @Override
                    public void coGroup(Iterable<User> iterable, Iterable<User> iterable1, Collector<String> collector) throws Exception {
                        collector.collect(iterable + "==>" + iterable1);
                    }
                }).print();

        env.execute();
    }

}
