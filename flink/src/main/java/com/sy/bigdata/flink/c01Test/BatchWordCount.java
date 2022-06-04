package com.sy.bigdata.flink.c01Test;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: sy
 * @Date: Created by 2022.4.26-14:32
 * @description:
 */
public class BatchWordCount {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = env.readTextFile("E:\\item\\bigdata\\flink\\src\\main\\resources\\document.txt");

        FlatMapOperator<String, Tuple2<String, Long>> tuple = stringDataSource.flatMap(new WordCountFlatMap());

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = tuple.groupBy(0);


        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        sum.print();


    }


}
