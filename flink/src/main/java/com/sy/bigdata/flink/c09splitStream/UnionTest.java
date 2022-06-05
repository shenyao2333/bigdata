package com.sy.bigdata.flink.c09splitStream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sy
 * @Date: Created by 2022.6.5-17:25
 * @description: union合流，类型需要一样，如果有水位线，想象成上游多个并行任务，使用最小值的水位线
 */
public class UnionTest {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.fromElements("1", "2", "3", "4");
        DataStreamSource<String> source2 = env.fromElements("一", "二", "三", "四");
        DataStream<String> union = source1.union(source2);
        union.print();
        env.execute();

    }

}
