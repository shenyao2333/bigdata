package com.sy.bigdata.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.Char;

/**
 * @Author: sy
 * @Date: Created by 2022.4.26-14:38
 * @description:
 */
public class LetterCountFlatMap implements FlatMapFunction<String, Tuple2<String,Long>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<String,Long>> collector) {
        for (char c : s.toCharArray()) {
            collector.collect(Tuple2.of(c+"", 1L));
        }

    }
}
