package com.sy.bigdata.flink.c05transform;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @Author: sy
 * @Date: Created by 2022.5.14-23:47
 * @description:
 */
public class FilterTest {

    public static void main(String[] args) throws Exception {


        DataStreamSource<User> env = CommonParam.getEnv();


         env.filter(user -> "老王".equals(user.name)).print();

         env.getExecutionEnvironment().execute();

    }
}
