package com.sy.bigdata.flink.c05transform;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.SimpleDateFormat;

/**
 * @Author: sy
 * @Date: Created by 2022.5.14-21:46
 * @description:
 */
public class MapTest {


    public static void main(String[] args) throws Exception {
        DataStreamSource<User> env = CommonParam.getEnv();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SingleOutputStreamOperator<String> map = env.map(user ->

                user.name + "在" + sdf.format(user.time) + " 游览了 " + user.url
        );
        map.print();
        env.getExecutionEnvironment().execute();
    }

}
