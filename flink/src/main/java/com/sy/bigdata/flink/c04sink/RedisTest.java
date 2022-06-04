package com.sy.bigdata.flink.c04sink;

import com.sy.bigdata.flink.common.CommonParam;
import com.sy.bigdata.flink.common.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author: sy
 * @Date: Created by 2022.5.17-11:19
 * @description:
 */
public class RedisTest {

    public static void main(String[] args) throws Exception {
        DataStreamSource<User> env = CommonParam.getEnv();

        FlinkJedisPoolConfig jedis = new FlinkJedisPoolConfig.Builder()
                .setHost("xxxx")
                .setPort(3306)
                .setPassword("123").build();

        env.addSink(new RedisSink<>(jedis,new MyRedisMapper()));
        env.getExecutionEnvironment().execute();


    }

    public static class MyRedisMapper implements RedisMapper<User> {


        @Override
        public RedisCommandDescription getCommandDescription() {
            /**
             * 使用哈希类型存所有数据的，  user_info 作为哈希表的key
             */
            return new RedisCommandDescription(RedisCommand.HSET,"user_info");
        }

        /**
         * key
         * @param user
         * @return
         */
        @Override
        public String getKeyFromData(User user) {
            return user.getName();
        }

        @Override
        public String getValueFromData(User user) {
            return user.getUrl();
        }
    }

}
