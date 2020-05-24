package com.fh.daml.datawarehouse.operation.entity;

/**
 * Author:zwj
 * Date:2019/12/11 9:19
 * Description:
 * Modified By:
 */
public class TableStatistics {

    //实体英文名
    private String ENAME;
    //实体中文名
    private String CNAME;
    //数据层级名称
    private String LEVEL_NAME;
    //实体数据量
    private String TABLE_NUM;

    private String JOB_ID;

    private Boolean importantFlag = Boolean.FALSE;

    private Boolean countException = Boolean.FALSE;

    public String getENAME() {
        return ENAME;
    }

    public void setENAME(String ENAME) {
        this.ENAME = ENAME;
    }

    public String getCNAME() {
        return CNAME;
    }

    public void setCNAME(String CNAME) {
        this.CNAME = CNAME;
    }

    public String getLEVEL_NAME() {
        return LEVEL_NAME;
    }

    public void setLEVEL_NAME(String LEVEL_NAME) {
        this.LEVEL_NAME = LEVEL_NAME;
    }

    public String getTABLE_NUM() {
        return TABLE_NUM;
    }

    public void setTABLE_NUM(String TABLE_NUM) {
        this.TABLE_NUM = TABLE_NUM;
    }

    public String getJOB_ID() {
        return JOB_ID;
    }

    public void setJOB_ID(String JOB_ID) {
        this.JOB_ID = JOB_ID;
    }

    public Boolean getImportantFlag() {
        return importantFlag;
    }

    public void setImportantFlag(Boolean importantFlag) {
        this.importantFlag = importantFlag;
    }

    public Boolean getCountException() {
        return countException;
    }

    public void setCountException(Boolean countException) {
        this.countException = countException;
    }
}
