package com.sy.bigdata.flink.transform;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

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
