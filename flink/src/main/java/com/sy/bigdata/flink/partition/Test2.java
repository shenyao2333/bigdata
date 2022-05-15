package com.sy.bigdata.flink.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import scala.Int;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-19:26
 * @description:
 */
public class Test2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RichParallelSourceFunction<Integer>() {

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    System.out.println( "i =="+i  + "分发给：" +getRuntimeContext().getIndexOfThisSubtask());
                    if (i%2 ==  getRuntimeContext().getIndexOfThisSubtask()){
                        System.out.println("确定分发："+ i + "  -- " + getRuntimeContext().getIndexOfThisSubtask());
                        sourceContext.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale()
        .print().setParallelism(4)
        ;

        env.execute();
    }
}
