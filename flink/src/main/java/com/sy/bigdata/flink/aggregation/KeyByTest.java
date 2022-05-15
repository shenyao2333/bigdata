package com.sy.bigdata.flink.aggregation;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-13:15
 * @description:
 */
public class KeyByTest {


    public static void main(String[] args) throws Exception {
        DataStreamSource<User> env = CommonParam.getEnv();

        KeyedStream<User, String> userStringKeyedStream = env.keyBy(user -> user.name);


        userStringKeyedStream.print("key_by -> ").setParallelism(3);

        env.getExecutionEnvironment().execute();
    }

}
