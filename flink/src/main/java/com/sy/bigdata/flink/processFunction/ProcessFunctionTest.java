package com.sy.bigdata.flink.processFunction;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: sy
 * @Date: Created by 2022.6.4-10:08
 * @description:  富函数使用案例
 */
public class ProcessFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<User> dataStreamSource = env.addSource(new CustomSource());

        SingleOutputStreamOperator<User> streamOperator = dataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ZERO).
                        withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                            @Override
                            public long extractTimestamp(User user, long l) {
                                return user.time.getTime();
                            }
                        })

        );

        SingleOutputStreamOperator<String> process = streamOperator.process(new ProcessFunction<User, String>() {
            /**
             * 每来一条数都会执行一次
             * @param user
             * @param context
             * @param collector
             * @throws Exception
             */
            @Override
            public void processElement(User user, Context context, Collector<String> collector) throws Exception {
                if ("小一".equals(user.name)){
                    collector.collect(user.name +"进入"+user.url);
                }else if ("张三".equals(user.name)){
                    collector.collect("监控对象: "+user.name +"进入"+user.url);
                }else {
                    collector.collect("其他用户进入: "+user.name);
                }
                Long timestamp = context.timestamp();
                System.out.println("事件时间： "+ timestamp);
                System.out.println("水位线时间："+ context.timerService().currentWatermark());
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println("当前处理solt索引号"+indexOfThisSubtask);
            }

        });

        process.print();
        env.execute();



    }




}
