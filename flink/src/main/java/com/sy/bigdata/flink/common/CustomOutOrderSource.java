package com.sy.bigdata.flink.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

/**
 * @Author: sy
 * @Date: Created by 2022.5.25-22:28
 * @description:  乱序流
 */
public class CustomOutOrderSource implements SourceFunction<User> {


    private Boolean running = true;

    @Override
    public void run(SourceContext<User> sourceContext) throws Exception {

        ArrayList<User> users = new ArrayList<>();

        // 李华 ，小红，  王五 迟到用户2 迟到用户1 张三
        users.add(new User("李华","/ind",new Date(1L)));
        users.add(new User("小红","/ind",new Date(2L)));
        users.add( new User("迟到用户1","/ind",new Date(6)));
        users.add(new User("迟到用户2","/ind",new Date(3)));
        users.add(new User("王五","/ind",new Date(7)));
        users.add(new User("张三2","/ind",new Date(8)));
        users.add(new User("张三","/ind",new Date(9)));
        Random random = new Random();
        int i = 0;
        //生成数据
        while (i<7){
            Thread.sleep(200);
            sourceContext.collect(users.get(i++));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
