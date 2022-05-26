package com.sy.bigdata.flink.window;

import com.sy.bigdata.flink.common.CustomOutOrderSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author: sy
 * @Date: Created by 2022.5.26-16:22
 * @description: 乱序流 处理
 */
public class WindowsOutOrderTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomOutOrderSource());

        SingleOutputStreamOperator<User> userData = userDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<User>forBoundedOutOfOrderness(
                                // 允许2毫秒的延迟 封窗口计算
                                Duration.ofMillis(2)).withTimestampAssigner((item, s) -> {
                            return item.time.getTime();
                        }));


        userData.print("data");


        userData.map(user -> Tuple2.of(user.getName(),1)).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(user -> true)
                // [0,3) 的数据
                .window(TumblingEventTimeWindows.of(Time.milliseconds(4))  )
                .sum(1).print("结果计算");

        env.execute();


    }
}
