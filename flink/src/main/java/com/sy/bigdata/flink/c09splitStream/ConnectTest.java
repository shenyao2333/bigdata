package com.sy.bigdata.flink.c09splitStream;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * @Author: sy
 * @Date: Created by 2022.6.5-17:19
 * @description:
 */
public class ConnectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> source2 = env.fromElements("一", "二", "三", "四");


        source2.connect(source1).map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s+" 字符按";
            }

            @Override
            public String map2(Integer integer) throws Exception {
                return integer + "数字";
            }
        }).print();



        // 还可以利用key相同，分配到同一个逻辑使用状态处理复杂的业务
        source1.connect(source2)
                .keyBy(data->data, data2->data2)
                .process(new CoProcessFunction<Integer, String, Object>() {
                    @Override
                    public void processElement1(Integer integer, Context context, Collector<Object> collector) throws Exception {

                    }

                    @Override
                    public void processElement2(String s, Context context, Collector<Object> collector) throws Exception {

                    }
        }).print();

        env.execute();

    }

}
