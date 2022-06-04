package com.sy.bigdata.flink.c07window;

import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;

/**
 * @Author: sy
 * @Date: Created by 2022.6.3-21:29
 * @description:  迟到数据综合测试  +  侧输出流 。
 *  测试的场景是统计每个访问页面的次数
 */
public class LastDataTest {


    // 小明,index,1000
    // 小红,home,2000
    // 小明,index,3000
    // 李华,cat,4000
    // 王五,user,5000
    // 小明,goods,8000
    // 张三,test,12000
    // 张三,index,12000
    // 王五,test,22000
    // 小明,index,24000
    // 小忘,index,72000

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("123.56.97.130", 8889);

        streamSource.print("数据来了--->");
        SingleOutputStreamOperator<User> map =  streamSource.map(s -> {
            String[] split = s.split(",");
            return new User(split[0], split[1], Long.parseLong(split[2]));
        });

        SingleOutputStreamOperator<User> userData = map
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        //允许两秒的延迟触发计算，也就是说12秒触发一次计算
                        .<User>forBoundedOutOfOrderness( Duration.ofSeconds(2)).withTimestampAssigner(
                                new SerializableTimestampAssigner<User>() {
                                    @Override
                                    public long extractTimestamp(User user, long l) {
                                        return user.getTime().getTime();
                                    }
                                }
                        ));

        OutputTag<User> late = new OutputTag<User>("late"){};

        SingleOutputStreamOperator<String> aggregate = userData.keyBy(user -> user.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //允许一分钟的迟到的数据回到窗口计算， 加上延迟2秒计算，所以水位线在62秒，窗口将不在计算。
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new Uv(), new UvCountByWindow());
        aggregate.print("10秒结束-->");
        aggregate.getSideOutput(late).print("迟到数据-->");
        env.execute();
    }


    /**
     * 这里有三个泛型:  输入对象，中间聚合状态，输出
     */
    public static class Uv  implements AggregateFunction<User, HashMap<String, Integer>,String > {

        @Override
        public HashMap<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Integer> add(User user, HashMap<String, Integer> hashMap) {
            String url = user.getUrl();
            Integer num = hashMap.get(url);
            if (num == null){
                hashMap.put(url,1);
            }else {
                hashMap.put(url,num+1);
            }
            return hashMap;
        }

        @Override
        public String getResult(HashMap<String, Integer> map) {
            StringBuilder sb = new StringBuilder();
            for (String url : map.keySet()) {
                sb.append("访问地址：").append(url).append("次数：").append(map.get(url)).append("\n");
            }
            return sb.toString();
        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> stringIntegerHashMap, HashMap<String, Integer> acc1) {
            return null;
        }

    }

    /**
     *
     * 输入的泛型； 输出的泛型；  分区的Key； 窗口信息；
     */
    public static class UvCountByWindow extends ProcessWindowFunction<String,String,String, TimeWindow> {

        /**
         *
         * @param aBoolean 分区key
         * @param context  上下文 ，可以拿到窗口、状态等信息
         * @param iterable 输入数据集合
         * @param collector 输出
         * @throws Exception
         */
        @Override
        public void process(String aBoolean, Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {

            String next = iterable.iterator().next();
            TimeWindow window = context.window();

            long start = window.getStart();
            long end = window.getEnd();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            collector.collect("窗口关闭->" +dateFormat.format(new Date(start))   + " ~ " + dateFormat.format(new Date(end)) +" 计算为：\n"+next);


        }
    }




}
