package com.fh.daml.datawarehouse.operation.base;

public class CheckResultTypeConstants {
    //运维工具操作类型
    public static final String[] TOOL_ACTION_TYPE = {"CHECK","COUNT"};
    //运维工具操作类型：统计
    public static final String TOOL_COUNT_TYPE = "COUNT";

    //运维工具统计类型
    public static final String[] TOOL_COUNT_DETAIL_TYPE = {"JOB","TABLE"};
    //运维工具统计类型:JOB
    public static final String TOOL_COUNT_JOB_TYPE = "JOB";
    //运维工具统计类型:TABLE
    public static final String TOOL_COUNT_TABLE_TYPE = "TABLE";


    //获取血缘关系返回的成功状态
    public static final int  BLOOD_RELATIONSHIP_SUCCESS_CODE= 200;
}
