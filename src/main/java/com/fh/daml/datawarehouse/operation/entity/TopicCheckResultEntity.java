package com.fh.daml.datawarehouse.operation.entity;

import java.math.BigDecimal;

public class TopicCheckResultEntity {

    //统计的主题库
    private String topic;

    //统计的主题库项目
    private String item;

    //数据量
    private BigDecimal count;

    //统计的主题库项目数据存储表名
    private String tableEName;

    //JOB_ID
    private String jobId;

    public String getTableEName() {
        return tableEName;
    }

    public void setTableEName(String tableEName) {
        this.tableEName = tableEName;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }



    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public BigDecimal getCount() {
        return count;
    }

    public void setCount(BigDecimal count) {
        this.count = count;
    }
}
