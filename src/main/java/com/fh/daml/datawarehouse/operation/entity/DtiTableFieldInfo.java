package com.fh.daml.datawarehouse.operation.entity;

/**
 * @file DtiTableColumnInfo
 * @version: 1.0
 * @Description: TODO
 * 代码目的，作用，如何工作
 * @Author: Administrator
 * @Date: 2019/7/15 11:26
 * 本代码要注意的事项、备注事项等。
 **/

public class DtiTableFieldInfo {
    /**
     * 字段英文名
     */
    private String fieldEnglishName;
    /**
     * 字段中文名
     */
    private String fieldChineseName;
    /**
     *字段总行数
     */
    private String fieldAllRows;
    /**
     * 字段有值行数
     */
    private String fieldValuableRows;
    /**
     * 字段错误行数
     */
    private String fieldErrorRows;
    /**
     * 错误占比
     */
    private String ErrorPro;

    /**
     * 有值率
     */
    private String perCent;
    /**
     *有值率的参考准则
     */
    private String percentRule;

    /**
     *是否正常
     */
    private String isNormal;
    /**
     * 是否忽略
     */
    private String isIgnore;

    //忽略原因
    private String ignoreReason;
    /**
     * 重要类型
     *
     */
    private String importantLevel;
    /**
     *
     * 字段属性
     */
    private String type;
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    public String getImportantLevel() {
        return importantLevel;
    }

    public void setImportantLevel(String importantLevel) {
        this.importantLevel = importantLevel;
    }


    public String getIsIgnore() {
        return isIgnore;
    }

    public void setIsIgnore(String isIgnore) {
        this.isIgnore = isIgnore;
    }

    public String getIgnoreReason() {
        return ignoreReason;
    }

    public void setIgnoreReason(String ignoreReason) {
        this.ignoreReason = ignoreReason;
    }


    public String getPerCent() {
        return perCent;
    }

    public void setPerCent(String perCent) {
        this.perCent = perCent;
    }

    public String getPercentRule() {
        return percentRule;
    }

    public void setPercentRule(String percentRule) {
        this.percentRule = percentRule;
    }

    public String getIsNormal() {
        return isNormal;
    }

    public void setIsNormal(String isNormal) {
        this.isNormal = isNormal;
    }


    public String getFieldValuableRows() {
        return fieldValuableRows;
    }

    public void setFieldValuableRows(String fieldValuableRows) {
        this.fieldValuableRows = fieldValuableRows;
    }

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

    public String getFieldAllRows() {
        return fieldAllRows;
    }

    public void setFieldAllRows(String fieldAllRows) {
        this.fieldAllRows = fieldAllRows;
    }

    public String getFieldErrorRows() {
        return fieldErrorRows;
    }

    public void setFieldErrorRows(String fieldErrorRows) {
        this.fieldErrorRows = fieldErrorRows;
    }

    public String getErrorPro() {
        return ErrorPro;
    }

    public void setErrorPro(String errorPro) {
        ErrorPro = errorPro;
    }

    @Override
    public String toString() {
        return "DtiTableFieldInfo{" +
                "fieldEnglishName='" + fieldEnglishName + '\'' +
                ", fieldChineseName='" + fieldChineseName + '\'' +
                ", fieldAllRows='" + fieldAllRows + '\'' +
                ", fieldValuableRows='" + fieldValuableRows + '\'' +
                ", fieldErrorRows='" + fieldErrorRows + '\'' +
                ", ErrorPro='" + ErrorPro + '\'' +
                '}';
    }
}
