package com.sy.bigdata.flink.sink;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * @Author: sy
 * @Date: Created by 2022.5.16-21:57
 * @description:
 */
public class FileTest {

    public static void main(String[] args) throws Exception {
        DataStreamSource<User> env = CommonParam.getEnv();


        //配置文件命名规则
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("flink-")
                .withPartSuffix(".txt")
                .build();

        //配置文件的规则，最大10M ， 最大间隔10分钟，空闲间隔5分钟
        DefaultRollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.builder()
                .withMaxPartSize(1024L * 1024 * 1024 * 10)
                .withRolloverInterval(1000 * 60 * 10)
                .withInactivityInterval(1000 * 60 * 5)
                .build();

        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("./out"),
                new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(config)
                .build();


        env.map(User::toString).addSink(fileSink);

        env.getExecutionEnvironment().execute();


    }

}
