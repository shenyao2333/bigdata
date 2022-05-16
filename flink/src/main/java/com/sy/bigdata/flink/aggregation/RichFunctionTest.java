package com.sy.bigdata.flink.aggregation;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import scala.Int;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-19:11
 * @description:
 */
public class RichFunctionTest {

    public static void main(String[] args) throws Exception {


        DataStreamSource<User> env = CommonParam.getEnv();

        env.map(new MyRichMapper()).setParallelism(4    ).print();

        env.getExecutionEnvironment().execute();
    }


    public static class MyRichMapper extends RichMapFunction<User, String> {


        @Override
        public void open(Configuration configuration) throws Exception {
            super.open(configuration);
            System.out.println("open 生命周期被调用: "+getRuntimeContext().getIndexOfThisSubtask()+ " 号启动任务");
        }

        @Override
        public String map(User user) throws Exception {

            return user.name;
        }


        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close 生命周期被调用: "+getRuntimeContext().getIndexOfThisSubtask()+ " 号关闭任务");
        }


    }
}
