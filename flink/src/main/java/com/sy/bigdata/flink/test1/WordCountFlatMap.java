package com.sy.bigdata.flink.test1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: sy
 * @Date: Created by 2022.4.26-14:38
 * @description:
 */
public class WordCountFlatMap implements FlatMapFunction<String, Tuple2<String,Long>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<String,Long>> collector) {
        String[] split = s.split(" ");
        for (String word : split) {
            collector.collect(Tuple2.of(word, 1L));
        }
    }
}
