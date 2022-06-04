package com.sy.bigdata.flink.common;

import lombok.Data;

/**
 * @Author: sy
 * @Date: Created by 2022.6.4-19:51
 * @description: 页面访问次数
 */
@Data
public class UrlViewCount {

    public Integer count;


    public String url;

    public Long endTime;

    public Long startTime;

    public UrlViewCount(String url, Integer count ,Long startTime ,  Long endTime ) {
        this.count = count;
        this.url = url;
        this.endTime = endTime;
        this.startTime = startTime;
    }

    public UrlViewCount() {
    }
}
