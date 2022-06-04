package com.sy.bigdata.flink.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.xml.transform.Source;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

/**
 * @Author: sy
 * @Date: Created by 2022.5.25-22:28
 * @description:
 */
public class CustomSource  implements SourceFunction<User> {


    private Boolean running = true;

    @Override
    public void run(SourceContext<User> sourceContext) throws Exception {


        String[] url = {"/home","/index","/cart","/goods","/user","/article"};
        String[] name = {"李华","小红","田七","王五","张三","小一"};

        Random random = new Random();
        //生成数据
        while (running){
            Thread.sleep(random.nextInt(3)* 700L);
            sourceContext.collect(new User( name[random.nextInt(6)], url[random.nextInt(6)],  new Date()));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
