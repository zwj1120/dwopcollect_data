package com.fh.daml.datawarehouse.operation.entity;

/**
 * @file TableColumnInfo
 * @version: 1.0
 * @Description: TODO
 * 代码目的，作用，如何工作
 * @Author: zwj
 * @Date: 2019/7/15 11:26
 * 本代码要注意的事项、备注事项等。
 **/

public class TableFieldInfo {
    /**
     * 字段英文名
     */
    private String fieldEnglishName;
    /**
     * 字段中文名
     */
    private String fieldChineseName;
    /**
     * 字段有值行数
     */
    private String fieldValuableRows;
    /**
     * 字段ID
     */
    private String FIELD_ID;

    private String TABLE_ENAME;

    public String getFieldEnglishName() {
        return fieldEnglishName;
    }

    public void setFieldEnglishName(String fieldEnglishName) {
        this.fieldEnglishName = fieldEnglishName;
    }

    public String getFieldChineseName() {
        return fieldChineseName;
    }

    public void setFieldChineseName(String fieldChineseName) {
        this.fieldChineseName = fieldChineseName;
    }

    public String getFieldValuableRows() {
        return fieldValuableRows;
    }

    public void setFieldValuableRows(String fieldValuableRows) {
        this.fieldValuableRows = fieldValuableRows;
    }

    public String getFIELD_ID() {
        return FIELD_ID;
    }

    public void setFIELD_ID(String FIELD_ID) {
        this.FIELD_ID = FIELD_ID;
    }

    public String getTABLE_ENAME() {
        return TABLE_ENAME;
    }

    public void setTABLE_ENAME(String TABLE_ENAME) {
        this.TABLE_ENAME = TABLE_ENAME;
    }
}
