package com.sy.bigdata.flink.c08processFunction;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author: sy
 * @Date: Created by 2022.6.4-11:44
 * @description: 事件时间定时器， 这里需要注意的是水位线的问题，  某时间的10分10秒的定时器，需要等到水位线过了10分10秒的才能触发这个定时器。
 */
public class EventTimeTimerTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<User> dataStreamSource = env.addSource(new CustomSource());

        SingleOutputStreamOperator<User> streamOperator = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((user, s) -> user.time.getTime()));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss-SSS");
        SingleOutputStreamOperator<String> process = streamOperator.keyBy(user -> user.name)
                // 三个泛型，keyBy的key类型，输入类型，输出类型
                .process(new KeyedProcessFunction<String, User, String>() {
                             @Override
                             public void processElement(User user, Context context, Collector<String> collector) throws Exception {
                                 long time = context.timestamp();
                                 collector.collect(user.getName()+"在 "+ time +"  进来"+ user.getUrl());
                                 collector.collect("水位线"+ context.timerService().currentWatermark());
                                 if ("张三".equals(context.getCurrentKey())){
                                     //注册一个定时器
                                     context.timerService().registerEventTimeTimer(time+10000);
                                     collector.collect("添加一个定时器");
                                 }

                             }

                             @Override
                             public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                 out.collect(ctx.getCurrentKey() + "进来的定时器，时间为：" + timestamp);
                                 out.collect("进来的定时器水位线"+ ctx.timerService().currentWatermark());
                             }
                         }
                );

        process.print();

        env.execute();

    }


}
