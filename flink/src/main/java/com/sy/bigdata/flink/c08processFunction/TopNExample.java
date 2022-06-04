package com.sy.bigdata.flink.c08processFunction;

import com.sy.bigdata.flink.common.CustomSource;
import com.sy.bigdata.flink.common.UrlViewCount;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * @Author: sy
 * @Date: Created by 2022.6.4-19:55
 * @description: 页面访问最大数实战
 */
public class TopNExample {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomSource());
        userDataStreamSource.print("数据来了-->");

        SingleOutputStreamOperator<User> streamOperator = userDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.
                <User>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((user, t) -> {
                    return user.time.getTime();
                }));


        SingleOutputStreamOperator<UrlViewCount> aggregate = streamOperator.keyBy(user -> user.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlAggregate(), new UrlWindow());


        SingleOutputStreamOperator<String> process = aggregate.keyBy(urlViewCount -> urlViewCount.endTime)
                .process(new TopNFunction(5));

        process.print();

        env.execute();


    }

    //
    public static class TopNFunction extends KeyedProcessFunction<Long,UrlViewCount, String>{
        Integer topN ;
        private ListState<UrlViewCount> dataList;

        public TopNFunction(Integer topN) {
            this.topN = topN;
        }


        @Override
        public void processElement(UrlViewCount urlViewCount, Context context, Collector<String> collector) throws Exception {
            dataList.add(urlViewCount);
            context.timerService().registerEventTimeTimer(context.timestamp()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ArrayList<UrlViewCount> data = new ArrayList<>();

            Iterable<UrlViewCount> urlViewCounts = dataList.get();
            for (UrlViewCount urlViewCount : urlViewCounts) {
                data.add(urlViewCount);
            }
            //排序
            data.sort( (url1,url2) ->{
                return  url2.count - url1.count ;
            });

            StringBuilder sb = new StringBuilder("");
            sb.append("时间段：").append(dateFormat.format(new Date(timestamp))).append(" 的前").append(topN).append("名为：\n");
            for (int i = 0; i < topN && i<data.size(); i++) {

                sb.append("第").append(i + 1).append("名：").append(data.get(i).url).append("，  数量为")
                        .append(data.get(i).count).append("\n");
            }
            out.collect(sb.toString());

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 固定格式这样写，  url-count-list 是生命一个状态名称
            dataList = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list",
                            Types.POJO(UrlViewCount.class))
            );
        }
    }


    public static class MyTreeMap extends TreeMap<Integer,String> {


        @Override
        public Comparator<? super Integer> comparator() {
            return super.comparator();
        }
    }


    public static class  UrlAggregate  implements AggregateFunction<User, HashMap<String,Integer>, HashMap<String,Integer>> {


        @Override
        public HashMap<String, Integer> createAccumulator() {
            return new HashMap<String, Integer>(2);
        }

        @Override
        public HashMap<String, Integer> add(User user, HashMap<String, Integer> map) {
            if (map.containsKey(user.getUrl())){
                map.put(user.getUrl(),map.get(user.getUrl())+1);
            }else {
                map.put(user.getUrl(),1);
            }
            return map;
        }

        @Override
        public HashMap<String, Integer> getResult(HashMap<String, Integer> stringIntegerHashMap) {
            return stringIntegerHashMap;
        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> stringIntegerHashMap, HashMap<String, Integer> acc1) {
            return null;
        }
    }




    public static class UrlWindow extends ProcessWindowFunction<HashMap<String,Integer>,UrlViewCount,String, TimeWindow> {


        @Override
        public void process(String url, Context context, Iterable<HashMap<String, Integer>> iterable, Collector<UrlViewCount> collector) throws Exception {
            HashMap<String, Integer> next = iterable.iterator().next();
            TimeWindow window = context.window();
            UrlViewCount urlViewCount = new UrlViewCount(url, next.get(url), window.getStart(), window.getEnd());
            collector.collect(urlViewCount);
        }
    }



}
