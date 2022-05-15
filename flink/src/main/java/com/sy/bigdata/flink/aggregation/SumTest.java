package com.sy.bigdata.flink.aggregation;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-18:09
 * @description:
 */
public class SumTest {

    /**
     * 找出哪个访问的多，最多的是多次
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        DataStreamSource<User> env = CommonParam.getEnv();

        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = env.map(user -> Tuple2.of(user.name, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = returns.keyBy(t -> t.f0)
                .sum(1);


        SingleOutputStreamOperator<Tuple2<String, Integer>> max = sum.keyBy(s -> "1")
                .maxBy(1);

        max.print();

        env.getExecutionEnvironment().execute();


    }

}
