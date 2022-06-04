package com.sy.bigdata.flink.c05transform;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @Author: sy
 * @Date: Created by 2022.5.14-23:50
 * @description:
 */
public   class  FlatMapTest {


    public static void main(String[] args) throws Exception {


        DataStreamSource<User> env = CommonParam.getEnv();



        // 写法一、
        SingleOutputStreamOperator<String> streamOperator = env.flatMap(new MyFlatMapFunction());



        // 写法二、
        //SingleOutputStreamOperator<String> streamOperator = env.flatMap((User in, Collector<String> out) -> {
        //    out.collect(in.name);
        //    out.collect(in.url);
        //}).returns(Types.STRING);


        streamOperator.print();
        env.getExecutionEnvironment().execute();
    }



    /**
     * 扁平映射 ， 将一个元素拆分为多个元素来输出
     */
    public  static class  MyFlatMapFunction implements FlatMapFunction<User, String> {
        SimpleDateFormat dateFormat  =   new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        @Override
        public void flatMap(User user, Collector<String> collector) throws Exception {
            collector.collect(user.name);
            collector.collect(user.url);
            collector.collect(dateFormat.format(user.time));
        }
    }


}
