package com.sy.bigdata.flink.common;

import lombok.Data;

import java.util.Date;

/**
 * @Author: sy
 * @Date: Created by 2022.5.14-23:32
 * @description:
 */
@Data
public class User {

    public String name;

    public String  url;

    public Date  time;

    public User() {

    }

    public User(String name, String url, Date time) {
        this.name = name;
        this.url = url;
        this.time = time;
    }
}
