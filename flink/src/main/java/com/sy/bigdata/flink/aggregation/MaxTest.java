package com.sy.bigdata.flink.aggregation;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-18:44
 * @description:
 */
public class MaxTest {

    /**
     * 找出用户最为活跃的数据
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        DataStreamSource<User> env = CommonParam.getEnv();
        /**
         * 使用name作为分组，求time最大，这里使用字段名去处理
         * max和 maxBy的区别：
         * 它们都是求最大值函数，返回的数据结构不改变，需要传入一个属性最为比较依据，
         * 然后根据这个字段做对比返回最大的对象， 而其他字段值max不处理，取第一次的属性值；maxBy是直接返回整个对象值
         */
        env.keyBy(user ->user.name).maxBy("time").print();

        env.getExecutionEnvironment().execute();

    }
}
