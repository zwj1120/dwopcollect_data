package com.fh.daml.datawarehouse.operation.entity;

import java.util.ArrayList;

public class ElementReportEntity {
    private String eleName;
    private String eleCount;
    private ArrayList<ElementReportEntity> relEleList = new ArrayList<ElementReportEntity>();

    public String getEleName() {
        return eleName;
    }

    public void setEleName(String eleName) {
        this.eleName = eleName;
    }

    public String getEleCount() {
        return eleCount;
    }

    public void setEleCount(String eleCount) {
        this.eleCount = eleCount;
    }

    public ArrayList<ElementReportEntity> getRelEleList() {
        return relEleList;
    }

    public void setRelEleList(ArrayList<ElementReportEntity> relEleList) {
        this.relEleList = relEleList;
    }
}
