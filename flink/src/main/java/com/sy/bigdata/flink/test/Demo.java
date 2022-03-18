package com.sy.bigdata.flink.test;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: sy
 * @Date: Created by 2022/3/11 15:29
 * @description:
 */
public class Demo {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        DataSource<String> stringDataSource = env.readTextFile("G:\\item\\bigdata\\flink\\src\\main\\resources\\document.txt");

        DataSet<Tuple2<String, Integer>> sum =
                stringDataSource.flatMap(new DefFlatMap()).groupBy(0)
                .sum(1);
        sum.print();


    }


}
