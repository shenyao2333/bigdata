package com.sy.bigdata.flink.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @Author: sy
 * @Date: Created by 2022/3/11 15:41
 * @description:
 */
public class DefFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {



    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = s.split(" ");
        for (String word : words) {
            collector.collect( new Tuple2<>(word,1));
        }

    }
}
