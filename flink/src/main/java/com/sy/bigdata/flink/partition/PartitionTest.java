package com.sy.bigdata.flink.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sy
 * @Date: Created by 2022.5.16-20:23
 * @description: 自定义分区
 */
public class PartitionTest {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4);

        /**
         * 传入两个对象
         * 1：
         * 2：分区需要依赖一个key，返回一个key组为分区的依据
         */
        streamSource.partitionCustom(new Partitioner<Integer>() {

            /**
             *
             * @param key 分区依据的key
             * @param numPartitions 可并行的任务数
             * @return
             */
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 4;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(4);

        env.execute();
    }




}
