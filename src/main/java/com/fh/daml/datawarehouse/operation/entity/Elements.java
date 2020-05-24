package com.fh.daml.datawarehouse.operation.entity;

/**
 * Author:zwj
 * Date:2020/1/6 10:44
 * Description:
 * Modified By:
 */
public class Elements {

    private String elementOne;
    private String elementTwo;
    private String countOne;
    private String countTwo;


    public Elements(String elementOne, String elementTwo, String countOne, String countTwo) {
        this.elementOne = elementOne;
        this.elementTwo = elementTwo;
        this.countOne = countOne;
        this.countTwo = countTwo;
    }

    public String getElementOne() {
        return elementOne;
    }

    public void setElementOne(String elementOne) {
        this.elementOne = elementOne;
    }

    public String getElementTwo() {
        return elementTwo;
    }

    public void setElementTwo(String elementTwo) {
        this.elementTwo = elementTwo;
    }

    public String getCountOne() {
        return countOne;
    }

    public void setCountOne(String countOne) {
        this.countOne = countOne;
    }

    public String getCountTwo() {
        return countTwo;
    }

    public void setCountTwo(String countTwo) {
        this.countTwo = countTwo;
    }


    @Override
    public String toString() {
        return "Elements{" +
                "elementOne='" + elementOne + '\'' +
                ", elementTwo='" + elementTwo + '\'' +
                ", countOne='" + countOne + '\'' +
                ", countTwo='" + countTwo + '\'' +
                '}';
    }
}
