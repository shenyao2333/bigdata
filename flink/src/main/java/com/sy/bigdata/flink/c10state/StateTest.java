package com.sy.bigdata.flink.c10state;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * @Author: sy
 * @Date: Created by 2022.6.9-22:04
 * @description:
 */
public class StateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        DataStreamSource<User> userStream = env.addSource(new CustomSource());

        userStream.keyBy(user -> user.name).flatMap( new MyFlatMap()).print();

        env.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<User, String> {

        private ValueState<User> userInfo ;
        private ListState<User> userList;
        private MapState<String,Integer> userCount;

        //聚合状态 两者都是只保存一个值， userReducing 传入user保存user。 userAgg传入user保存string
        private ReducingState<User> userReducing;
        private AggregatingState<User,String> userAgg;

        //如果是定义普通的本地变量，那么这个count不会和分区key有关系，会一直叠加上去。
        private Integer count ;



        @Override
        public void flatMap(User user, Collector<String> collector) throws Exception {

            userInfo.update(user);
           // collector.collect(user.toString());


            userList.add(user);

            userCount.put(user.url, userCount.contains(user.url)?userCount.get(user.url)+1:1);

            System.out.println("userCount: "+ user.name +  " 访问了"+user.url+" 次数："+  userCount.get(user.url));

            userAgg.add(user);
            System.out.println("userAgg: "+userAgg.get());

            userReducing.add(user);
            System.out.println("userReducing: " +userReducing.get());

            count++;
        }






        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<User> userInfo = new ValueStateDescriptor<>("userInfo", User.class);

            this.userInfo = getRuntimeContext().getState(userInfo);

            userList = getRuntimeContext().getListState(new ListStateDescriptor<User>("userList", User.class));
            userCount = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("userCount",String.class,Integer.class));

            // 输入，中间状态，输出（就是保存的类型） 三个泛型
            userAgg = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<User, Long, String>("userAgg", new AggregateFunction<User, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(User user, Long aLong) {
                    return aLong+1;
                }

                @Override
                public String getResult(Long aLong) {
                    return "输出 "+ aLong;
                }

                @Override
                public Long merge(Long aLong, Long acc1) {
                    return aLong + acc1;
                }
            }, Long.class));



            userReducing = getRuntimeContext().getReducingState(new ReducingStateDescriptor<User>("userReducing", new ReduceFunction<User>() {
                @Override
                public User reduce(User user, User t1) throws Exception {
                    return new User(user.name,user.url,t1.time);
                }
            }, User.class));

            count = 0 ;


            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    // 在什么情况下会更新它的失效时间默认为OnCreateAndWrite，意思是在创建或者修改的时候会重置为一小时失效
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    // 数据失效并不会立马清除，在没清楚的情况下，是否需要返回呢？默认使用NeverReturnExpired不返回
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            // 设置过期时间
            userInfo.enableTimeToLive(ttlConfig);
        }


    }

}
