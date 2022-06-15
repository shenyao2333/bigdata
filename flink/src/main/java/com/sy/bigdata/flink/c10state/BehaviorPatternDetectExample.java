package com.sy.bigdata.flink.c10state;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @Author: sy
 * @Date: Created by 2022.6.14-21:36
 * @description:
 */
public class BehaviorPatternDetectExample {

    public static void main(String[] args) throws Exception {


        SingleOutputStreamOperator<User> customEnv = CommonParam.getCustomEnv();
        //customEnv.print("数据进来->");
        StreamExecutionEnvironment env = customEnv.getExecutionEnvironment();


        DataStreamSource<Tuple2<String, String>> ruleSource = env.addSource( new RuleSource());

        MapStateDescriptor<Integer, Tuple2<String, String>> rule = new MapStateDescriptor<>("rule", Types.INT, Types.TUPLE(Types.STRING, Types.STRING));

        BroadcastStream<Tuple2<String, String>> broadcast = ruleSource.broadcast(rule);


        customEnv.keyBy(user -> user.name)
                .connect(broadcast)
                .process(new PatternDetector() ).print();



        env.execute();

    }

    public static class RuleSource implements SourceFunction<Tuple2<String,String>> {
        private String[] data = {"/cart","/goods","/user"};
        @Override
        public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
            Random random = new Random();

            while (true){
                sourceContext.collect(Tuple2.of(data[random.nextInt(3)],data[random.nextInt(3)]));
                Thread.sleep(10000);
            }
        }

        @Override
        public void cancel() {

        }
    }

        //KS, IN1, IN2, OUT
    public static class PatternDetector extends KeyedBroadcastProcessFunction<String,User,Tuple2<String,String>,String> {


            private ValueState<User> lastUser;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastUser =  getRuntimeContext().getState(new ValueStateDescriptor<User>("lastUser",User.class));

            }


            @Override
            public void processElement(User user, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                MapStateDescriptor<Integer, Tuple2<String,String>> rule = new MapStateDescriptor<>("rule", Types.INT, Types.TUPLE(Types.STRING, Types.STRING));
                ReadOnlyBroadcastState<Integer, Tuple2<String,String>> broadcastState = readOnlyContext.getBroadcastState(rule);
                Tuple2<String,String> tuple = broadcastState.get(1);
                User lastValue = lastUser.value();
                if (lastValue!=null && tuple!=null ){
                    if (lastValue.url.equals(tuple.f0) && user.getUrl().equals(tuple.f1)){
                        collector.collect("成功匹配---> 该次：" + user +"，上一次："+lastValue+ "； 规则为："+tuple  );
                    }
                }
                lastUser.update(user);

            }

            @Override
            public void processBroadcastElement(Tuple2<String, String> stringStringTuple2, Context context, Collector<String> collector) throws Exception {
                System.out.println("规则发生改变："+stringStringTuple2);
                BroadcastState<Integer, Tuple> rule = context.getBroadcastState(new MapStateDescriptor<>("rule", Types.INT, Types.TUPLE(Types.STRING, Types.STRING)));
                rule.put(1,stringStringTuple2);

            }
        }

}
