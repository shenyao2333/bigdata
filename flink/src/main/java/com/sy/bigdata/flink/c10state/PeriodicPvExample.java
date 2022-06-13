package com.sy.bigdata.flink.c10state;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: sy
 * @Date: Created by 2022.6.10-16:59
 * @description: 需求， 统计每段时间计数，但不想来一条就输出一条
 *  所以结合定时器 + 状态 来实现
 */
public class PeriodicPvExample {

    public static void main(String[] args) throws Exception {

        SingleOutputStreamOperator<User> env = CommonParam.getCustomEnv();
        env.print("数据来了-->");
        env.keyBy(user -> user.name)
                .process( new PeriodicPvFunction())
                .print();


        env.getExecutionEnvironment().execute();

    }

    public static class PeriodicPvFunction extends      KeyedProcessFunction<String ,User, String> {


        private ValueState<Integer> count;


        @Override
        public void open(Configuration parameters) throws Exception {
            count  = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count",Integer.class));
        }




        @Override
        public void processElement(User user, Context context, Collector<String> collector) throws Exception {
            Integer value = count.value();
            if (value == null){
                value = 0;
                //第一次进来的时候注册一个定时器
                long time = user.time.getTime();
                System.out.println("第一次进来的"+ time);
                context.timerService().registerEventTimeTimer(user.time.getTime()+10000);
            }
            count.update(value+1);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect("计数处理-》" +ctx.timerService().currentWatermark() + ctx.getCurrentKey()+ "的次数为："+count.value());

            ctx.timerService().registerEventTimeTimer(timestamp+10000);

        }

    }




}
