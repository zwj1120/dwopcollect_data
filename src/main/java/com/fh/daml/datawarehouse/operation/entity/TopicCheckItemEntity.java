package com.fh.daml.datawarehouse.operation.entity;

import java.math.BigDecimal;

public class TopicCheckItemEntity {

    //统计的主题库
    private String topic;

    //统计的主题库项目
    private String item;

    //统计的主题库项目数据存储表名
    private String tableEName;

    //执行策略
    private String strategy;

    //操作类型：count,统计;check,检测
    private String actionType;

    //统计类型：job，任务统计；table，表统计
    private String countType;

    //JOB_ID
    private String jobId;

    //数据量
    private BigDecimal count;

    public String getTopic() {
        return topic;
    }

    public TopicCheckItemEntity setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getItem() {
        return item;
    }

    public TopicCheckItemEntity setItem(String item) {
        this.item = item;
        return this;
    }

    public String getTableEName() {
        return tableEName;
    }

    public TopicCheckItemEntity setTableEName(String tableEName) {
        this.tableEName = tableEName;
        return this;
    }

    public String getStrategy() {
        return strategy;
    }

    public TopicCheckItemEntity setStrategy(String strategy) {
        this.strategy = strategy;
        return this;
    }

    public String getActionType() {
        return actionType;
    }

    public TopicCheckItemEntity setActionType(String actionType) {
        this.actionType = actionType;
        return this;
    }

    public String getCountType() {
        return countType;
    }

    public TopicCheckItemEntity setCountType(String countType) {
        this.countType = countType;
        return this;
    }

    public String getJobId() {
        return jobId;
    }

    public TopicCheckItemEntity setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public BigDecimal getCount() {
        return count;
    }

    public TopicCheckItemEntity setCount(BigDecimal count) {
        this.count = count;
        return this;
    }
}
