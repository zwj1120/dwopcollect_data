package com.fh.daml.datawarehouse.operation.dao;

import com.fh.daml.datawarehouse.operation.base.CommonConstants;
import com.fh.daml.datawarehouse.operation.base.StatisticsSqlConstants;
import com.fh.daml.datawarehouse.operation.entity.DiffAreaFieldCount;
import com.fh.daml.datawarehouse.operation.entity.DiffAreaTableInfo;
import com.fh.daml.datawarehouse.operation.entity.DtiTableFieldInfo;
import com.fh.daml.datawarehouse.operation.entity.ElementReportEntity;
import com.fh.daml.datawarehouse.operation.utils.DateUtils;
import com.fh.daml.datawarehouse.operation.utils.MysqlJdbcTemplateUtil;
import com.fh.daml.datawarehouse.operation.utils.SqliteJdbcTemplateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Author:zwj
 * Date:2020/1/14 14:22
 * Description:
 * Modified By:
 */
public class ToolDao {



    private final static Logger logger = Logger.getLogger(ToolDao.class);

    //删除本周的数据
    public void deleteWeekData()throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String format = sdf.format(new SimpleDateFormat("yyyy-MM-dd")
                .parse(DateUtils.weekStart(0).toString()));

        String sqlT = "DELETE FROM T_NUM WHERE ENTER_TIME > "+format;
        String sqlF = "DELETE FROM F_NUM WHERE ENTER_TIME > "+format;
        SqliteJdbcTemplateUtil.getInstance().executeUpdate(sqlT);
        SqliteJdbcTemplateUtil.getInstance().executeUpdate(sqlF);
    }


    //获取各数据层级的表数据信息,并插入Sqlite中
    public void importDataLevelInfo(String levelName,String format)throws Exception{

        String sql ="";
        //数据缓冲层
        if (CommonConstants.STG_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.STG_TABLE_QUALITY_SQL;
        }
        //数据标准层
        else if (CommonConstants.ODS_NAME.equals(levelName)){
            sql = StatisticsSqlConstants.ODS_TABLE_QUALITY_SQL;
        }
        //数据明细层
        else if(CommonConstants.DWD_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.DWD_TABLE_QUALITY_SQL;
        }
        //数据汇总层
        else if(CommonConstants.DWS_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.DWS_TABLE_INFO_SQL;
        }
        //数据维度层
        else if (CommonConstants.DIM_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.DIM_TABLE_INFO_SQL;
        }
        //数据应用层
        else if (CommonConstants.ADM_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.ADM_TABLE_INFO_SQL;
        }

        if (StringUtils.isEmpty(sql)) {
            return;
        }
        List<DiffAreaTableInfo> diffAreaTableInfos = MysqlJdbcTemplateUtil.getInstance().queryForList(DiffAreaTableInfo.class, sql);
        String city = SqliteJdbcTemplateUtil.getInstance().queryForMap("SELECT city From CONFIG").get("city").toString();
        String cityNo = SqliteJdbcTemplateUtil.getInstance().queryForMap("SELECT CITYNO From CONFIG").get("CITYNO").toString();

        //拼接SQL,进行批量插入操作
        int index = 0;
        StringBuilder sb = new StringBuilder();
        int len = diffAreaTableInfos.size();
        for (DiffAreaTableInfo diffAreaTableInfo:diffAreaTableInfos) {

            if (len == 1) {
                sb.append("INSERT INTO T_NUM VALUES('"+diffAreaTableInfo.getENAME().replace('\'','"')
                        +"','"+diffAreaTableInfo.getCNAME().replace('\'','"')+"','"
                        +diffAreaTableInfo.getLEVEL().replace('\'','"')+"','"
                        +diffAreaTableInfo.getTABLE_NUM().replace('\'','"')
                        +"','"+city+"','"+cityNo+"','"+format+"')");
            }else {
                if (index == 0) {
                    sb.append("INSERT INTO T_NUM VALUES('"+diffAreaTableInfo.getENAME().replace('\'','"')
                            +"','"+diffAreaTableInfo.getCNAME().replace('\'','"')+"','"
                            +diffAreaTableInfo.getLEVEL().replace('\'','"')+"','"
                            +diffAreaTableInfo.getTABLE_NUM().replace('\'','"')
                            +"','"+city+"','"+cityNo+"','"+format+"'),");
                }else if (index == len -1) {
                    sb.append("('"+diffAreaTableInfo.getENAME().replace('\'','"')
                            +"','"+diffAreaTableInfo.getCNAME().replace('\'','"')+"','"
                            +diffAreaTableInfo.getLEVEL().replace('\'','"')+"','"
                            +diffAreaTableInfo.getTABLE_NUM().replace('\'','"')
                            +"','"+city+"','"+cityNo+"','"+format+"')");
                }else {
                    sb.append("('"+diffAreaTableInfo.getENAME().replace('\'','"')
                            +"','"+diffAreaTableInfo.getCNAME().replace('\'','"')+"','"
                            +diffAreaTableInfo.getLEVEL().replace('\'','"')+"','"
                            +diffAreaTableInfo.getTABLE_NUM().replace('\'','"')
                            +"','"+city+"','"+cityNo+"','"+format+"'),");
                }
            }
            index++;
        }
        if(!StringUtils.isEmpty(sb.toString())) {
            SqliteJdbcTemplateUtil.getInstance().executeUpdate(sb.toString());
        }
    }



    //导入字段的信息
    public void importTableFieldInfo(String levelName,String format)throws Exception{
        String sql = "";
        if (CommonConstants.STG_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.STG_TABLE_FIELD_INFO_CRUST;
        }
        else if (CommonConstants.ODS_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.ODS_TABLE_FIELD_INFO_CRUST;
        }
        else if (CommonConstants.DWD_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.DWD_TABLE_FIELD_INFO_CRUST;
        }
        else if (CommonConstants.DWS_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.DWS_TABLE_FIELD_INFO_SQL;
        }
        else if (CommonConstants.DIM_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.DIM_TABLE_FIELD_INFO_SQL;
        }
        else if (CommonConstants.ADM_NAME.equals(levelName)) {
            sql = StatisticsSqlConstants.ADM_TABLE_FIELD_INFO_SQL;
        }
        if (StringUtils.isEmpty(sql)) {
            return;
        }
        List<DiffAreaFieldCount> diffAreaFieldCounts = MysqlJdbcTemplateUtil.getInstance().queryForList(DiffAreaFieldCount.class, sql);

        String cityNo = SqliteJdbcTemplateUtil.getInstance().queryForMap("SELECT CITYNO From CONFIG").get("CITYNO").toString();

        int index = 0;
        StringBuilder sb = new StringBuilder();
        int len = diffAreaFieldCounts.size();
        for (DiffAreaFieldCount diffAreaFieldCount:diffAreaFieldCounts) {

            if (len == 1) {
                sb.append("INSERT INTO F_NUM VALUES('"+diffAreaFieldCount.getENAME().replace('\'','"')+"','"
                        +diffAreaFieldCount.getLEVEL().replace('\'','"')+"','"+diffAreaFieldCount.getF_ENAME().replace('\'','"')+"','"
                        +diffAreaFieldCount.getF_CNAME().replace('\'','"')+"','"+diffAreaFieldCount.getF_COUNT().replace('\'','"')
                        +"','"+diffAreaFieldCount.getSEQUENCE_NUM().replace('\'','"')+"','"+cityNo+"','"+format+"')");
            }else {
                if (index == 0) {
                    sb.append("INSERT INTO F_NUM VALUES('"+diffAreaFieldCount.getENAME().replace('\'','"')+"','"
                            +diffAreaFieldCount.getLEVEL().replace('\'','"')+"','"+diffAreaFieldCount.getF_ENAME().replace('\'','"')+"','"
                            +diffAreaFieldCount.getF_CNAME().replace('\'','"')+"','"+diffAreaFieldCount.getF_COUNT().replace('\'','"')
                            +"','"+diffAreaFieldCount.getSEQUENCE_NUM().replace('\'','"')+"','"+cityNo+"','"+format+"'),");
                }else if (index == len -1) {
                    sb.append("('"+diffAreaFieldCount.getENAME().replace('\'','"')+"','"
                            +diffAreaFieldCount.getLEVEL().replace('\'','"')+"','"+diffAreaFieldCount.getF_ENAME().replace('\'','"')+"','"
                            +diffAreaFieldCount.getF_CNAME().replace('\'','"')+"','"+diffAreaFieldCount.getF_COUNT().replace('\'','"')
                            +"','"+diffAreaFieldCount.getSEQUENCE_NUM().replace('\'','"')+"','"+cityNo+"','"+format+"')");
                }else {
                    sb.append("('"+diffAreaFieldCount.getENAME().replace('\'','"')+"','"
                            +diffAreaFieldCount.getLEVEL().replace('\'','"')+"','"+diffAreaFieldCount.getF_ENAME().replace('\'','"')+"','"
                            +diffAreaFieldCount.getF_CNAME().replace('\'','"')+"','"+diffAreaFieldCount.getF_COUNT().replace('\'','"')
                            +"','"+diffAreaFieldCount.getSEQUENCE_NUM().replace('\'','"')+"','"+cityNo+"','"+format+"'),");
                }
            }
            index++;
        }
        if (!StringUtils.isEmpty(sb.toString())) {
            SqliteJdbcTemplateUtil.getInstance().executeUpdate(sb.toString());
        }

    }


    /**
     * 获取人员主题库NB_APP_ADM_PER_BASE_S的字段有值率情况
     * @return
     * @throws Exception
     */
    public List<DtiTableFieldInfo> getPersonFieldRate()throws Exception{

        String sql = "SELECT NAME AS\n" +
                "\tfieldChineseName,\n" +
                "\tENAME AS fieldEnglishName,\n" +
                "\tIFNULL(inputCnt,0) AS fieldValuableRows \n" +
                "FROM\n" +
                "\t(\n" +
                "\tSELECT\n" +
                "\t\tTRIM( BOTH '\"' FROM json_extract( DIMENSION, '$[2]' ) ) AS fieldCode,\n" +
                "\t\tDIMENSION_VALUE AS inputCnt \n" +
                "\tFROM\n" +
                "\t\tSTATISTICS_LOG \n" +
                "\tWHERE\n" +
                "\t\tPROCESS_ID='d61290ac-e08e-4465-ae07-d80db84de33c'\n" +
                "\t\tAND json_extract( DIMENSION, '$[0]' ) = '_tgt' \n" +
                "\t\tAND json_extract( DIMENSION, '$[1]' ) = '04' \n" +
                "\t\tAND\tTARGET_ID <> -1\n" +
                "\t\tAND TIMESTAMP = (\n" +
                "\t\tSELECT\n" +
                "\t\t\tMAX( `TIMESTAMP` ) \n" +
                "\t\tFROM\n" +
                "\t\t\tSTATISTICS_LOG \n" +
                "\t\tWHERE\n" +
                "\t\t\tPROCESS_ID='d61290ac-e08e-4465-ae07-d80db84de33c'\n" +
                "\t\t\tAND\tTARGET_ID <> -1\n" +
                "\t\t\tAND json_extract( DIMENSION, '$[0]' ) = '_tgt' \n" +
                "\t\t\tAND json_extract( DIMENSION, '$[1]' ) = '04' \n" +
                "\t\t) \n" +
                "\t) C\n" +
                "\tRIGHT JOIN BASE_FIELD_INFO B ON C.fieldCode = B.Field_id \n" +
                "WHERE\n" +
                "\tB.TEMPLATE_ID = ( SELECT TEMPLATE_ID FROM BASE_ENTITY_INFO WHERE ENAME = 'NB_APP_ADM_PER_BASE_S' LIMIT 1 ) \n" +
                "ORDER BY\n" +
                "\tB.Field_id ASC";
        return MysqlJdbcTemplateUtil.getInstance().queryForList(DtiTableFieldInfo.class, sql);
    }


    /**
     * 获取该地的人口总数
     *
     * @return
     * @Author:xd
     */
    public String getSumValue() {
        String num = "";
        try {
            num = SqliteJdbcTemplateUtil.getInstance().queryForBean(String.class, "SELECT NUM FROM CONFIG LIMIT 1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return num;
    }


    /**
     * 将关系主题库的数据量信息存入sqlite表中
     * @param result
     */
    public void saveRelationCountToSqlite(List<Map<String, Object>> result) throws Exception{
        if (null == result || 0 == result.size()) {
            logger.error("无关系主题库数据量信息！");
            return;
        }
        try {
            String deleteSql = "delete from relation_theme_res_statistics";
            if (SqliteJdbcTemplateUtil.getInstance().executeUpdate(deleteSql) < 0) {
                throw new Exception("删除Sqlite库中表relation_theme_res_statistics数据失败！");
            }

            String insertSql = "insert into relation_theme_res_statistics values";
            StringBuilder sb = new StringBuilder();
            for (Map<String, Object> d:result) {
                sb.append("(").append("'"+d.get("isNormal")+"',")
                        .append("'"+d.get("ignoreReason")+"',")
                        .append("'"+d.get("count")+"',")
                        .append("'"+d.get("range")+"',")
                        .append("'"+d.get("ignore")+"',")
                        .append("'"+d.get("type")+"',")
                        .append("'"+d.get("sumValue")+"',")
                        .append("'"+d.get("relation")+"'),");
            }

            insertSql = insertSql + sb.deleteCharAt(sb.length()-1).toString();
            SqliteJdbcTemplateUtil.getInstance().executeUpdate(insertSql);
        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    }


    /**
     * 获取资源库基础要素的统计信息
     * @return
     * @throws Exception
     */
    public List<Map<String, Object>> getAllElementsCount() throws Exception{
        String sql = "SELECT\n" +
                "\tbe.id,be.`NAME`,bf.field_id,bf.`NAME` as element, REPLACE(MAX(CONCAT(IFNULL(s.`TIMESTAMP`,'0000000000'),IFNULL(s.DIMENSION_VALUE,0))),MAX(IFNULL(s.`TIMESTAMP`,'0000000000')),'')  AS count\n" +
                "FROM\n" +
                "statistics_log s RIGHT JOIN\n" +
                "\tbase_field_info bf ON s.DIMENSION = CONCAT('[\"_tgt\",\"04\",\"',bf.field_id,'\"]')\n" +
                "\tJOIN base_entity_info be ON bf.template_id = be.template_id\n" +
                "\tJOIN dti_kinship_entity k ON be.id = k.OUTPUT_ENTITY_ID\n" +
                "\tJOIN dti_schedule_job j ON k.JOB_ID = j.JOB_ID \n" +
                "\tAND j.`DESC` LIKE '%要素基础表生成' \n" +
                "\tAND k.OUTPUT_ENTITY_ID > 0\n" +
                "\tGROUP BY be.id,bf.field_id,bf.`NAME`,be.`NAME`\n" +
                "\tORDER BY be.id,bf.field_id ASC";

        return MysqlJdbcTemplateUtil.getInstance().queryForListMap(sql);
    }

    /**
     * 将资源库基础要素的统计信息存储至sqlite表中
     * @param result
     * @throws Exception
     */
    public void saveBaseElementStatistics(ArrayList<ElementReportEntity> result)throws Exception {

        if (null == result || 0 == result.size()) {
            return;
        }
        try {
            String deleteSql = "delete from BASE_ELEMENT_STATISTICS";
            if (SqliteJdbcTemplateUtil.getInstance().executeUpdate(deleteSql) < 0) {
                throw new Exception("删除Sqlite库中表BASE_ELEMENT_STATISTICS数据失败！");
            }

            String insertSql = "insert into BASE_ELEMENT_STATISTICS values";
            StringBuilder sb = new StringBuilder();
            for (ElementReportEntity e1:result) {
                if (null != e1.getRelEleList() && 0 < e1.getRelEleList().size() ) {
                    for (ElementReportEntity e2:e1.getRelEleList()) {
                        sb.append("('"+e1.getEleName()+"','"+e1.getEleCount()+"','"+e2.getEleName()+"','"+e2.getEleCount()+"'),");
                    }
                }
            }

            insertSql = insertSql + sb.deleteCharAt(sb.length()-1).toString();
            SqliteJdbcTemplateUtil.getInstance().executeUpdate(insertSql);
        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    }


}
