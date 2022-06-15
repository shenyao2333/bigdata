package com.sy.bigdata.flink.c10state;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: sy
 * @Date: Created by 2022.6.13-21:34
 * @description:
 */
public class BufferingSinkExample {

    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<User> customEnv = CommonParam.getCustomEnv();

        customEnv.print("input");




        customEnv.addSink(new BufferingSink(5));


        customEnv.getExecutionEnvironment().execute();
    }


    public static class BufferingSink implements SinkFunction<User>, CheckpointedFunction{

        private final int threshold;

        private List<User> bufferElements;

        //定义一个算子状态
        private ListState<User> checkpointedState;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            bufferElements = new ArrayList<>();
        }


        @Override
        public void invoke(User value, Context context) throws Exception {
            // 缓存到列表
            bufferElements.add(value);
            // 如果达到阈值，就批量写入
            if (bufferElements.size() ==threshold ){
                // 打印到控制台模拟写入外部系统
                for (User bufferElement : bufferElements) {
                    System.out.println(bufferElement);
                }
                System.out.println("===========输出完毕===========");
                bufferElements.clear();
            }

        }


        // 持久化的时候调用的方法
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkpointedState.clear();
            System.out.println("--------持久话调用-------");
            // 对状态就行持久化，复制缓存的列表到列表状态
            for (User bufferElement : bufferElements) {
                checkpointedState.add(bufferElement);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            ListStateDescriptor<User> userListStateDescriptor = new ListStateDescriptor<>("buffered-element", User.class);

            checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(userListStateDescriptor);

            //如果从故障恢复，需要将ListState
            if (functionInitializationContext.isRestored()){
                for (User user : checkpointedState.get()) {
                    bufferElements.add(user);
                }
            }

        }
    }


}
