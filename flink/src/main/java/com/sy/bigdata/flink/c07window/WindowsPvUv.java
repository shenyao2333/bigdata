package com.sy.bigdata.flink.c07window;

import com.sy.bigdata.flink.common.CustomOutOrderSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @Author: sy
 * @Date: Created by 2022.5.25-22:40
 * @description: 增量计算pv  和uv   pv： 访问数，  uv：用户数
 */
public class WindowsPvUv {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  每100ms自动生成一个水位线
        //env.getConfig().setAutoWatermarkInterval(200);
        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomOutOrderSource());

        SingleOutputStreamOperator<User> userData = userDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy
                .<User>forBoundedOutOfOrderness(
                        // 允许
                        Duration.ofSeconds(3)).withTimestampAssigner((item, s) -> {
                    return item.time.getTime();
                }));


        userData.print("data");


        userData.keyBy( user -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))  )
                .aggregate( new PvUv()).print();

        env.execute();
    }

    /**
     * 这里有三个泛型:  输入对象，中间聚合状态，输出
     */
    public static class PvUv  implements AggregateFunction<User, Tuple2<Long, HashSet<String>>,Double > {

        /**
         * 初始化累加器
         * @return
         */
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L,new HashSet<>());
        }

        /**
         *
         * @param user
         * @param longHashSetTuple2
         * @return
         */
        @Override
        public Tuple2<Long, HashSet<String>> add(User user, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            longHashSetTuple2.f1.add(user.name);

            return Tuple2.of(longHashSetTuple2.f0+1,longHashSetTuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            return (double)  longHashSetTuple2.f0/longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            longHashSetTuple2.f1.addAll(acc1.f1);
            return Tuple2.of(longHashSetTuple2.f0+acc1.f0,longHashSetTuple2.f1);
        }
    }




}
