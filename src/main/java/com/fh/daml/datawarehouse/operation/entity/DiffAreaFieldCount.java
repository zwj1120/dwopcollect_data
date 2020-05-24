package com.fh.daml.datawarehouse.operation.entity;

import java.util.Map;

/**
 * Author:zwj
 * Date:2019/8/31 16:15
 * Description:
 * Modified By:
 */
public class DiffAreaFieldCount {
    private String ENAME;
    private String LEVEL;
    private String F_ENAME;
    private String F_CNAME;
    private String F_COUNT;
    private String SEQUENCE_NUM;
    private String AREA;
    private String ENTER_TIME;


    public DiffAreaFieldCount() {

    }

    public DiffAreaFieldCount(Map map) {
        this.ENAME = map.get("ENAME").toString();
        this.LEVEL = map.get("LEVEL").toString();
        this.F_ENAME = map.get("F_ENAME").toString();
        this.F_CNAME = map.get("F_CNAME").toString();
        this.F_COUNT = map.get("F_COUNT").toString();
        this.SEQUENCE_NUM = map.get("SEQUENCE_NUM").toString();
        this.AREA = map.get("AREA").toString();
        this.ENTER_TIME = map.get("ENTER_TIME").toString();
    }

    public String getENAME() {
        return ENAME;
    }

    public void setENAME(String ENAME) {
        this.ENAME = ENAME;
    }

    public String getLEVEL() {
        return LEVEL;
    }

    public void setLEVEL(String LEVEL) {
        this.LEVEL = LEVEL;
    }

    public String getF_ENAME() {
        return F_ENAME;
    }

    public void setF_ENAME(String f_ENAME) {
        F_ENAME = f_ENAME;
    }

    public String getF_CNAME() {
        return F_CNAME;
    }

    public void setF_CNAME(String f_CNAME) {
        F_CNAME = f_CNAME;
    }

    public String getF_COUNT() {
        return F_COUNT;
    }

    public void setF_COUNT(String f_COUNT) {
        F_COUNT = f_COUNT;
    }

    public String getSEQUENCE_NUM() {
        return SEQUENCE_NUM;
    }

    public void setSEQUENCE_NUM(String SEQUENCE_NUM) {
        this.SEQUENCE_NUM = SEQUENCE_NUM;
    }

    public String getAREA() {
        return AREA;
    }

    public void setAREA(String AREA) {
        this.AREA = AREA;
    }

    public String getENTER_TIME() {
        return ENTER_TIME;
    }

    public void setENTER_TIME(String ENTER_TIME) {
        this.ENTER_TIME = ENTER_TIME;
    }
}
