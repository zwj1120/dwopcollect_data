package com.fh.daml.datawarehouse.operation.base;

/**
 * Author:zwj
 * Date:2020/1/14 14:29
 * Description:
 * Modified By:
 */
public class CommonConstants {

    public static final String ODS_NAME = "数据标准层";

    public static final String DWD_NAME = "数据明细层";

    public static final String DWS_NAME = "数据汇总层";

    public static final String DIM_NAME = "数据维度层";

    public static final String ADM_NAME = "数据应用层";

    public static final String STG_NAME = "数据缓冲层";

    public static final String[] DTI_JOB_EXECUTE_STRATEGY = {"INC","FUL"};

    public static final String DTI_JOB_INCREASE_STRATEGY = "INC";

    public static final char SPLIT_CHAR = '\u0016';
}
