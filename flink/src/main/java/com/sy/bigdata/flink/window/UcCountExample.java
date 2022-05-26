package com.sy.bigdata.flink.window;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;

/**
 * @Author: sy
 * @Date: Created by 2022.5.26-23:05
 * @description: 这里是增量聚合 +  窗口信息
 */
public class UcCountExample {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  每100ms自动生成一个水位线
        //env.getConfig().setAutoWatermarkInterval(200);
        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomSource());

        SingleOutputStreamOperator<User> userData = userDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<User>forBoundedOutOfOrderness(
                                // 允许
                                Duration.ofSeconds(3)).withTimestampAssigner((item, s) -> {
                            return item.time.getTime();
                        }));


        userData.print("data");


        userData.keyBy( user -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(5))  )
                .aggregate(new Uv(),new UvCountByWindow())
                .print();

        env.execute();
    }



    /**
     * 这里有三个泛型:  输入对象，中间聚合状态，输出
     */
    public static class Uv  implements AggregateFunction<User, HashSet<String>,Integer > {


        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(User user, HashSet<String> strings) {
            strings.add(user.name);
            return strings;
        }

        @Override
        public Integer getResult(HashSet<String> strings) {
            return strings.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }

    /**
     *
     * 输入的泛型； 输出的泛型；  分区的Key； 窗口信息；
     */
    public static class UvCountByWindow extends ProcessWindowFunction<Integer,String,Boolean, TimeWindow> {

        /**
         *
         * @param aBoolean 分区key
         * @param context  上下文 ，可以拿到窗口、状态等信息
         * @param iterable 输入数据集合
         * @param collector 输出
         * @throws Exception
         */
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {

            Integer next = iterable.iterator().next();
            TimeWindow window = context.window();

            long start = window.getStart();
            long end = window.getEnd();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            collector.collect("窗口" +dateFormat.format(new Date(start))   + " ~ " + dateFormat.format(new Date(end)) + "的UV值："+ next);
        }
    }



}
