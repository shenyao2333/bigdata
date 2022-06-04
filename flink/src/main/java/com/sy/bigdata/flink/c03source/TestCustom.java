package com.sy.bigdata.flink.c03source;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sy
 * @Date: Created by 2022.5.25-22:25
 * @description:  自定义 源
 */
public class TestCustom {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomSource());


        userDataStreamSource.print();

        env.execute();


    }








}
