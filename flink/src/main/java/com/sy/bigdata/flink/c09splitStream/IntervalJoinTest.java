package com.sy.bigdata.flink.c09splitStream;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @Author: sy
 * @Date: Created by 2022.6.6-21:42
 * @description: 间隔连接
 */
public class IntervalJoinTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<User> userStream = env.addSource(new CustomSource());


        SingleOutputStreamOperator<User> userStreamOperator = userStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(
                        (u, l) -> u.time.getTime())
        );

        userStream.print("用户游览数据来了-->");

        long nowTime = System.currentTimeMillis();

        DataStreamSource<Tuple2<String, Date>> orderStream = env.fromElements(
                Tuple2.of("李华", new Date(nowTime - 800)),
                Tuple2.of("小红", new Date(nowTime - 700)),
                Tuple2.of("李华", new Date(nowTime - 400)),
                Tuple2.of("田七", new Date(nowTime)),
                Tuple2.of("王五", new Date(nowTime + 1200)),
                Tuple2.of("王五", new Date(nowTime + 2100)),
                Tuple2.of("张三", new Date(nowTime + 2100)),
                Tuple2.of("小红", new Date(nowTime + 3500)),
                Tuple2.of("王五", new Date(nowTime + 4300)),
                Tuple2.of("小一", new Date(nowTime + 2100)),
                Tuple2.of("小一", new Date(nowTime + 4100))
        );

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        orderStream.map(s -> s.f0 +  dateFormat.format(s.f1)).print("订单数据来了-->");


        SingleOutputStreamOperator<Tuple2<String, Date>> orderStreamOperator = orderStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Date>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(
                        (u, l) -> u.f1.getTime()));




        orderStreamOperator.keyBy(u -> u.f0)
                .intervalJoin(userStreamOperator.keyBy(u -> u.name))
                .between(Time.milliseconds(-500), Time.milliseconds(500))
                .process(new ProcessJoinFunction<Tuple2<String, Date>, User, String>() {
                    @Override
                    public void processElement(Tuple2<String, Date> stringDateTuple2, User user, Context context, Collector<String> collector) throws Exception {
                        collector.collect("匹配上--》"+stringDateTuple2.f0 + dateFormat.format(stringDateTuple2.f1)+" ==> " + user);
                    }
                }).print();

        env.execute();

    }


}
