package com.fh.daml.datawarehouse.operation.service;

import com.fh.daml.datawarehouse.operation.dao.ToolDao;
import com.fh.daml.datawarehouse.operation.entity.DtiTableFieldInfo;
import com.fh.daml.datawarehouse.operation.utils.CalculateUtils;
import com.fh.daml.datawarehouse.operation.utils.SqliteJdbcTemplateUtil;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author:zwj
 * Date:2020/1/14 15:32
 * Description:
 * Modified By:
 */
public class PersonThemeBase {


    private static Map<String, Map<String, Object>> fieldRuleTable = new HashMap<>();
    private final static Logger logger = Logger.getLogger(PersonThemeBase.class);
    private static ToolDao toolDao = new ToolDao();

    static {
        fieldRuleTable.clear();
        try {
            List<Map<String, Object>> personFieldList = getPersonField();
            for (Map<String, Object> map : personFieldList) {
                fieldRuleTable.put(String.valueOf(map.get("fieldEnglishName")), map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //获取人员主题库字段情况
    private static List<Map<String, Object>> getPersonField() {
        List<Map<String, Object>> result = null;
        String sql = "select * from field_condition";
        try {
            result = SqliteJdbcTemplateUtil.getInstance().queryForListMap(sql);
            if (result.isEmpty()) {
                logger.info("获取人员主题库字段情况数为0");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("获取人员主题库字段情况SQL执行失败！！！");
            e.printStackTrace();
        }
        return result;
    }

    public Boolean savePersonFieldRate()throws Exception {

        try {
            String lineCount = "";
            List<DtiTableFieldInfo> personFieldRate = toolDao.getPersonFieldRate();
            for(DtiTableFieldInfo dtiTableFieldInfo:personFieldRate){
                if(dtiTableFieldInfo.getFieldEnglishName().equals("MD_ID")){
                    lineCount=dtiTableFieldInfo.getFieldValuableRows();
                    break;
                }
            }
            getfieldPercent(personFieldRate,lineCount,"NB_APP_ADM_PER_BASE_S","person");
            savePersonFieldRate(personFieldRate);
            return Boolean.TRUE;
        }catch (Exception e) {
            throw new Exception(e);
        }
    }

    public void getfieldPercent(List<DtiTableFieldInfo> dtiTableFieldInfos, String lineCount, String tableName, String type) {
        for (DtiTableFieldInfo dtiTableFieldInfo : dtiTableFieldInfos) {
            if (!fieldRuleTable.containsKey(dtiTableFieldInfo.getFieldEnglishName())) {
                dtiTableFieldInfo.setIsNormal("");
                continue;
            }
            dtiTableFieldInfo.setFieldAllRows(lineCount);
            dtiTableFieldInfo.setPerCent(CalculateUtils.getPercent(dtiTableFieldInfo.getFieldValuableRows(), lineCount));
            dtiTableFieldInfo.setPercentRule(String.valueOf(type.equals("person") ? fieldRuleTable.get(dtiTableFieldInfo.getFieldEnglishName()).get("PERSONRANGE") : fieldRuleTable.get(dtiTableFieldInfo.getFieldEnglishName()).get("RESIDENTRANGE")));
            dtiTableFieldInfo.setIsIgnore(String.valueOf(fieldRuleTable.get(dtiTableFieldInfo.getFieldEnglishName()).get("IGNORE")));
            dtiTableFieldInfo.setIgnoreReason(String.valueOf(fieldRuleTable.get(dtiTableFieldInfo.getFieldEnglishName()).get("IGNOREREASON")));
            dtiTableFieldInfo.setImportantLevel(String.valueOf(fieldRuleTable.get(dtiTableFieldInfo.getFieldEnglishName()).get("IMPORTANTLEVEL")));
            dtiTableFieldInfo.setType(String.valueOf(fieldRuleTable.get(dtiTableFieldInfo.getFieldEnglishName()).get("FIELDTYPE")));

            if (dtiTableFieldInfo.getPercentRule().contains("-")) {
                if (Double.parseDouble(dtiTableFieldInfo.getPerCent().substring(0, dtiTableFieldInfo.getPerCent().length() - 1)) >= Double.parseDouble(dtiTableFieldInfo.getPercentRule().split("-")[0])) {
                    dtiTableFieldInfo.setIsNormal("正常");
                } else {
                    dtiTableFieldInfo.setIsNormal("偏低");
                }
            } else if (dtiTableFieldInfo.getPercentRule().contains(">")) {
                if (Double.parseDouble(dtiTableFieldInfo.getPerCent().substring(0, dtiTableFieldInfo.getPerCent().length() - 1)) >= Double.parseDouble(dtiTableFieldInfo.getPercentRule().replace(">", ""))) {
                    dtiTableFieldInfo.setIsNormal("正常");
                } else {
                    dtiTableFieldInfo.setIsNormal("偏低");
                }
            } else {
                if (dtiTableFieldInfo.getPercentRule().equals("不检测")) {
                    dtiTableFieldInfo.setIsNormal("忽略");
                } else if (Double.parseDouble(dtiTableFieldInfo.getPerCent().substring(0, dtiTableFieldInfo.getPerCent().length() - 1)) >= Double.parseDouble(dtiTableFieldInfo.getPercentRule())) {
                    dtiTableFieldInfo.setIsNormal("正常");
                } else {
                    dtiTableFieldInfo.setIsNormal("偏低");
                }
            }
        }
    }




    /**
     * 保存人员主题库有值率情况至Sqlite
     * @param result
     */
    public void savePersonFieldRate(List<DtiTableFieldInfo> result)throws Exception {

        if (null == result || 0 == result.size()) {
            logger.error("无人员主题库有值率信息！");
            return;
        }

        try {
            String deleteSql = "delete from person_theme_field_rate";
            if (SqliteJdbcTemplateUtil.getInstance().executeUpdate(deleteSql) < 0) {
                throw new Exception("删除Sqlite库中表person_theme_field_rate数据失败！");
            }

            String insertSql = "insert into person_theme_field_rate values";
            StringBuilder sb = new StringBuilder();
            for (DtiTableFieldInfo d:result) {
                sb.append("(").append("'"+d.getFieldEnglishName()+"',")
                        .append("'"+d.getFieldChineseName()+"',")
                        .append("'"+d.getFieldAllRows()+"',")
                        .append("'"+d.getFieldValuableRows()+"',")
                        .append("'"+d.getFieldErrorRows()+"',")
                        .append("'"+d.getErrorPro()+"',")
                        .append("'"+d.getPerCent()+"',")
                        .append("'"+d.getPercentRule()+"',")
                        .append("'"+d.getIsNormal()+"',")
                        .append("'"+d.getIsIgnore()+"',")
                        .append("'"+d.getIgnoreReason()+"',")
                        .append("'"+d.getImportantLevel()+"',")
                        .append("'"+d.getType()+"'),");
            }

            insertSql = insertSql + sb.deleteCharAt(sb.length()-1).toString();
            SqliteJdbcTemplateUtil.getInstance().executeUpdate(insertSql);
        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    }


}
