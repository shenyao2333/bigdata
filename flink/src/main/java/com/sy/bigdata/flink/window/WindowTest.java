package com.sy.bigdata.flink.window;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;

/**
 * @Author: sy
 * @Date: Created by 2022.5.23-22:42
 * @description:
 */
public class WindowTest {


    public static void main(String[] args) throws Exception {

        DataStreamSource<User> env = CommonParam.getEnv();

        env
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<User>forBoundedOutOfOrderness(
                                Duration.ofMillis(10)).withTimestampAssigner( ( item , s )->{
                            return item.time.getTime();
                        }))

                .keyBy( user -> user.getName())

                .window(TumblingEventTimeWindows.of(Time.seconds(10)))  //滚动时间窗口
                //.window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(10))) // 滑动事件，1个小时 滑动距离
                //.window(EventTimeSessionWindows.withGap(Time.seconds(10)))  //事件时间会话窗口
               // .countWindow(10,2) // 滑动计数窗口
                .reduce( (s1,s2) -> {
                    s1.setTime(new Date(s1.getTime().getTime()+s2.getTime().getTime()));
                    return s1;
                } ).print();


        env.getExecutionEnvironment().execute();


    }


}
