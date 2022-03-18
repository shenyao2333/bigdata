package com.sy.bigdata.flink.test4;

import com.sy.bigdata.flink.test.DefFlatMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;


/**
 * @Author: sy
 * @Date: Created by 2022/3/18 16:16
 * @description:
 */
public class WordCount {


    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String path = parameterTool.get("path");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile(path);


        DataSet<Tuple2<String, Integer>> sum =
                dataSource.flatMap(new DefFlatMap())
                        .groupBy(0)
                        .sum(1);

        sum.print();




    }




}
