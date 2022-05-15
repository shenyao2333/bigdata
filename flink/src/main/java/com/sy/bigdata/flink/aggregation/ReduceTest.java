package com.sy.bigdata.flink.aggregation;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-18:56
 * @description:
 */
public class ReduceTest {

    public static void main(String[] args) throws  Exception {

        DataStreamSource<User> env = CommonParam.getEnv();


        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = env.map(user -> Tuple2.of(user.name, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)

                // s 为前者（前面归约好的对象），h为后面传入的对象
                .reduce((s, h) -> Tuple2.of(s.f0, s.f1 + h.f1)).returns(Types.TUPLE(Types.STRING, Types.INT));


        reduce.print();

        env.getExecutionEnvironment().execute();


    }
}
