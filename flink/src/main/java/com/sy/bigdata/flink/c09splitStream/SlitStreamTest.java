package com.sy.bigdata.flink.c09splitStream;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: sy
 * @Date: Created by 2022.6.5-10:24
 * @description: 分流操作示例
 */
public class SlitStreamTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomSource());
        SingleOutputStreamOperator<User> streamOperator = userDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((item, s) -> item.time.getTime()));

        OutputTag<User> tagZhang = new OutputTag<User>("tag_zhang"){};

        OutputTag<Tuple2<String,String>> tagLi= new OutputTag<Tuple2<String,String>>("tag_li"){};

        SingleOutputStreamOperator<User> process = streamOperator.process(new ProcessFunction<User, User>() {
            @Override
            public void processElement(User user, Context context, Collector<User> collector) throws Exception {
                if ("张三".equals(user.name)){
                    context.output(tagZhang,user);
                }else if (user.name.contains("李")){
                    context.output(tagLi,Tuple2.of(user.name,user.url));
                }else {
                    collector.collect(user);
                }

            }
        });

        process.getSideOutput(tagZhang).print("张三分流-->");
        process.getSideOutput(tagLi).print("李分流-->");
        process.print("主流-->");
        env.execute();

    }



}
