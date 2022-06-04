package com.sy.bigdata.flink.c06partition;

import com.sy.bigdata.flink.common.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-12:58
 * @description:
 */
public class ShuffleTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);


        DataStreamSource<User> streamSource = env.fromElements(
                new User("小明", "home", new Date(2342342)),
                new User("小红", "cart", new Date(23421143)),
                new User("老王", "index", new Date(2342343)),
                new User("老王", "home", new Date(24545343)),
                new User("小明", "cart", new Date(334564243)),
                new User("小红", "cart", new Date(344242343)),
                new User("老王", "goods", new Date(345534234)),
                new User("小明", "goods", new Date(395534234))
        );


        // 随机分区
       // streamSource.shuffle().print().setParallelism(4);


        //轮询分区
        streamSource.rebalance().print().setParallelism(4);


        env.execute();

    }


}
