package com.sy.bigdata.flink.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import scala.Int;

/**
 * @Author: sy
 * @Date: Created by 2022.5.15-19:26
 * @description: 重缩放分区 。 意味着某个状态节点发生了并发的缩扩，
 * 状态的数据分布将发生变化，因此存在一个reshuffle的过程
 */
public class RescaleTest {

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
            //源设置为两个并行任务，
        }).setParallelism(2)
                // 这里分配打印操作前设置了重缩放分区，四个并行任务，意味着将两个任务分为四个
                .rescale().print().setParallelism(4);

        env.execute();
    }
}
