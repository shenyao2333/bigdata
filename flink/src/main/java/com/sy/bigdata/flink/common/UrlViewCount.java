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


    private String url;

    public Long endTime;

    public Long startTime;


}
