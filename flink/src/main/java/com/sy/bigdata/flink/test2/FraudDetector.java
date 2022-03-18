package com.sy.bigdata.flink.test2;

import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Transaction;


/**
 * @Author: sy
 * @Date: Created by 2022/3/18 9:17
 * @description:
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;


    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());
        System.out.println(alert.toString());
        collector.collect(alert);
    }
}
