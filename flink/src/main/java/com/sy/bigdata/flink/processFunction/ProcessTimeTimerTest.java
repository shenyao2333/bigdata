package com.sy.bigdata.flink.processFunction;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @Author: sy
 * @Date: Created by 2022.6.4-11:10
 * @description: 注册定时器， 注意这里用的是事件的到达事件， 所以没配置事件时间的字段。
 */
public class ProcessTimeTimerTest {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<User> dataStreamSource = env.addSource(new CustomSource());


        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss-SSS");
        SingleOutputStreamOperator<String> process = dataStreamSource.keyBy(user -> user.name)
                // 三个泛型，keyBy的key类型，输入类型，输出类型
                .process(new KeyedProcessFunction<String, User, String>() {
                             @Override
                             public void processElement(User user, Context context, Collector<String> collector) throws Exception {
                                 long time = context.timerService().currentProcessingTime();
                                 collector.collect(user.getName()+"在"+ dateFormat.format(new Date(time)) +"进来"+ user.getUrl());
                                 if ("张三".equals(context.getCurrentKey())){
                                     //注册一个定时器
                                     context.timerService().registerProcessingTimeTimer(time+10000);
                                     collector.collect("添加一个定时器");
                                 }

                             }

                             @Override
                             public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                 out.collect(ctx.getCurrentKey() + "进来的定时器，时间为：" + dateFormat.format(new Date(timestamp)));

                             }
                         }
                );

        process.print();

        env.execute();



    }

}
