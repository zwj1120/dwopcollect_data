package com.fh.daml.datawarehouse.operation.entity;

/**
 * Author:zwj
 * Date:2019/11/13 9:30
 * Description:
 * Modified By:
 */
public class DiffAreaTableInfo {

    //英文表名
    private String ENAME;
    //中文表名
    private String CNAME;
    //数据层级
    private String LEVEL;
    //表数据量
    private String TABLE_NUM;
    //地区编码
    private String AREA;
    //地区中文名称
    private String AREA_NAME;
    //录入时间
    private String ENTER_TIME;

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

    public String getLEVEL() {
        return LEVEL;
    }

    public void setLEVEL(String LEVEL) {
        this.LEVEL = LEVEL;
    }

    public String getTABLE_NUM() {
        return TABLE_NUM;
    }

    public void setTABLE_NUM(String TABLE_NUM) {
        this.TABLE_NUM = TABLE_NUM;
    }

    public String getAREA() {
        return AREA;
    }

    public void setAREA(String AREA) {
        this.AREA = AREA;
    }

    public String getAREA_NAME() {
        return AREA_NAME;
    }

    public void setAREA_NAME(String AREA_NAME) {
        this.AREA_NAME = AREA_NAME;
    }

    public String getENTER_TIME() {
        return ENTER_TIME;
    }

    public void setENTER_TIME(String ENTER_TIME) {
        this.ENTER_TIME = ENTER_TIME;
    }
}
