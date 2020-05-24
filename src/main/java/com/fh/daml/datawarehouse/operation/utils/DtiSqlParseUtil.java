package com.fh.daml.datawarehouse.operation.utils;


import com.mysql.jdbc.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DtiSqlParseUtil {

    public static  List<String> getTableNamesBySql(String jobId) {

        try {

            if (StringUtils.isNullOrEmpty(jobId)) {
                return Collections.emptyList();
            }

            //根据JobId获取输入表的SQL
            String getInputTableSql = "SELECT DISTINCT\n" +
                    "\tt1.INPUT_ENTITY_NAME \n" +
                    "FROM\n" +
                    "\tdti_kinship_entity t1 \n" +
                    "WHERE\n" +
                    "\tt1.job_id = '"+jobId+"' \n" +
                    "\tAND t1.INPUT_ENTITY_ID != '-1'";

            //根据JobId获取输出表的SQL
            String getOutPutTableSql = "SELECT DISTINCT\n" +
                    "\tt1.OUTPUT_ENTITY_NAME \n" +
                    "FROM\n" +
                    "\tdti_kinship_entity t1 \n" +
                    "WHERE\n" +
                    "\tt1.job_id = '"+jobId+"' \n" +
                    "\tAND t1.OUTPUT_ENTITY_ID != '-1'";
            List<String> result =  new ArrayList<>();


            List<String> outputList = MysqlJdbcTemplateUtil.getInstance().queryForList(String.class, getOutPutTableSql);
            List<String> inputList = MysqlJdbcTemplateUtil.getInstance().queryForList(String.class, getInputTableSql);

            if (outputList.size() == 0 || inputList.size() ==0) {
                return Collections.emptyList();
            }
            result.addAll(outputList);
            result.addAll(inputList);
            return result;
        }catch (SQLException e) {
            return Collections.emptyList();
        }
    }
}
