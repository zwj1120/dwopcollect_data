package com.fh.daml.datawarehouse.operation.base;

import com.fh.daml.datawarehouse.operation.utils.ConfUtil;

/**
 * Author:zwj
 * Date:2019/12/9 16:58
 * Description:数据盘点功能模块所需SQL
 * Modified By:
 */
public class StatisticsSqlConstants {

    //获取数据缓冲层的相关表信息
    public final static String STG_TABLE_INFO_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据缓冲层'  AS `LEVEL`,\n" +
            "\tIFNULL( SUM( t.dimension_value ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.target = CONCAT_WS( '.', '"+ ConfUtil.pro.getProperty("CHECK_DATASOURCE")+"', b.ename ) \n" +
//            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.target = CONCAT_WS( '.', 'GOVERNANCE_HADOOP_CLUSTER', b.ename ) \n" +
            "\tAND t.dimension_value > 0 \n" +
            "\tAND t.target_id = b.id \n" +
            "\tAND t.target IS NOT NULL \n" +
            "\tAND t.dimension = '[\"_tgt\",\"01\"]'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tAND b.ENAME LIKE '%STG_%'\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据缓冲层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    //获取数据标准层相关表信息
    public final static String ODS_TABLE_INFO_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据标准层'  AS `LEVEL`,\n" +
            "\tIFNULL( SUM( t.dimension_value ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.target = CONCAT_WS( '.', '"+ConfUtil.pro.getProperty("CHECK_DATASOURCE")+"', b.ename ) \n" +
            "\tAND t.dimension_value > 0 \n" +
            "\tAND t.target_id = b.id \n" +
            "\tAND t.target IS NOT NULL \n" +
            "\tAND t.dimension = '[\"_tgt\",\"01\"]'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tAND b.ENAME LIKE '%ODS_%'\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据标准层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    //获取数据明细层相关表信息
    public final static String DWD_TABLE_INFO_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据明细层'  AS `LEVEL`,\n" +
            "\tIFNULL( SUM( t.dimension_value ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.target = CONCAT_WS( '.', '"+ConfUtil.pro.getProperty("CHECK_DATASOURCE")+"', b.ename ) \n" +
            "\tAND t.dimension_value > 0 \n" +
            "\tAND t.target_id = b.id \n" +
            "\tAND t.target IS NOT NULL \n" +
            "\tAND t.dimension = '[\"_tgt\",\"01\"]'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tAND b.ENAME LIKE '%DWD_%'\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据明细层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    //获取数据汇总层相关表信息
    public final static String DWS_TABLE_INFO_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据汇总层'  AS `LEVEL`,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target = CONCAT_WS( '.', '"+ConfUtil.pro.getProperty("CHECK_DATASOURCE")+"', b.ename ) \n" +
            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension = '[\"_tgt\",\"01\"]'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id \n" +
            "\tAND b.ENAME LIKE '%DWS_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据汇总层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    //获取数据维度层相关表信息
    public final static String DIM_TABLE_INFO_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据维度层'  AS `LEVEL`,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target = CONCAT_WS( '.', '"+ConfUtil.pro.getProperty("CHECK_DATASOURCE")+"', b.ename ) \n" +
            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension = '[\"_tgt\",\"01\"]'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id \n" +
            "\tAND b.ENAME LIKE '%DIM_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据维度层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    //获取数据应用成相关表信息
    public final static String ADM_TABLE_INFO_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据应用层'  AS `LEVEL`,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target = CONCAT_WS( '.', '"+ConfUtil.pro.getProperty("CHECK_DATASOURCE")+"', b.ename ) \n" +
            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension = '[\"_tgt\",\"01\"]'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id \n" +
            "\tAND b.ENAME LIKE '%ADM_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据应用层' \n" +
            "GROUP BY\n" +
            "\tb.ename\t";

    //获取数据缓冲层所有表的字段信息
    public final static String STG_TABLE_FIELD_INFO_SQL = "\tSELECT\n" +
            "\tt1.ENAME AS ENAME,\n" +
            "\tt1.TEMPLATE_ID,\n" +
            "\t'数据缓冲层' AS `LEVEL`,\n" +
            "\tb.ENAME AS F_ENAME,\n" +
            "\tb.NAME AS F_CNAME,\n" +
            "\tIFNULL( SUM( t.dimension_value ), 0 ) AS F_COUNT,\n" +
            "\tb.FIELD_ID AS SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_FIELD_INFO b ON  t.dimension = CONCAT( '[\"_tgt\",\"04\",\"',b.Field_ID,'\"]')\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id  \n" +
            "\t-- 下面一句可有可无\n" +
            "\tAND t1.ENAME LIKE '%STG_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据缓冲层' \n" +
            "GROUP BY\n" +
            "\tt1.TEMPLATE_ID,b.ename";

    public final static String STG_TABLE_FIELD_INFO_CRUST = "SELECT\n" +
            "  IFNULL(JSON_EXTRACT(JSON_EXTRACT(f.quality_info,concat('$.\"',bf.field_id,'\"')),'$.\"correct\"'),0) AS F_COUNT,\n" +
            "\tb.ename AS ENAME,\n" +
            "\t'数据缓冲层' AS `LEVEL`,\n" +
            "\tbf.ename as F_ENAME,\n" +
            "\tbf.name as F_CNAME,\n" +
            "\tbf.field_id as SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tentity_field_quality_info f \n" +
            "\tRIGHT JOIN entity_quality_info t ON f.ENTITY_QUALITY_ID = t.id\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.ENTITY_ID = b.id\n" +
            "\tRIGHT JOIN base_entity_extends bfe ON b.id = bfe.entity_id and bfe.key = 'column.name' \n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tjoin base_field_info bf on b.template_id = bf.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据缓冲层' \n" +
            "\twhere instr(concat(',',bfe.VALUE,','),concat(',',bf.ename,',')) <> 0\n" +
            "GROUP BY\n" +
            "\tb.ename,bf.ename\n" +
            "\tORDER BY bf.FIELD_ID asc";


    public final static String ODS_TABLE_FIELD_INFO_CRUST = "SELECT\n" +
            "  IFNULL(JSON_EXTRACT(JSON_EXTRACT(f.quality_info,concat('$.\"',bf.field_id,'\"')),'$.\"correct\"'),0) AS F_COUNT,\n" +
            "\tb.ename AS ENAME,\n" +
            "\t'数据标准层' AS `LEVEL`,\n" +
            "\tbf.ename as F_ENAME,\n" +
            "\tbf.name as F_CNAME,\n" +
            "\tbf.field_id as SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tentity_field_quality_info f \n" +
            "\tRIGHT JOIN entity_quality_info t ON f.ENTITY_QUALITY_ID = t.id\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.ENTITY_ID = b.id\n" +
            "\tRIGHT JOIN base_entity_extends bfe ON b.id = bfe.entity_id and bfe.key = 'column.name' \n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tjoin base_field_info bf on b.template_id = bf.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据标准层' \n" +
            "\twhere instr(concat(',',bfe.VALUE,','),concat(',',bf.ename,',')) <> 0\n" +
            "GROUP BY\n" +
            "\tb.ename,bf.ename\n" +
            "\tORDER BY bf.FIELD_ID asc";


    public final static String DWD_TABLE_FIELD_INFO_CRUST = "SELECT\n" +
            "  IFNULL(JSON_EXTRACT(JSON_EXTRACT(f.quality_info,concat('$.\"',bf.field_id,'\"')),'$.\"correct\"'),0) AS F_COUNT,\n" +
            "\tb.ename AS ENAME,\n" +
            "\t'数据明细层' AS `LEVEL`,\n" +
            "\tbf.ename as F_ENAME,\n" +
            "\tbf.name as F_CNAME,\n" +
            "\tbf.field_id as SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tentity_field_quality_info f \n" +
            "\tRIGHT JOIN entity_quality_info t ON f.ENTITY_QUALITY_ID = t.id\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.ENTITY_ID = b.id\n" +
            "\tRIGHT JOIN base_entity_extends bfe ON b.id = bfe.entity_id and bfe.key = 'column.name' \n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tjoin base_field_info bf on b.template_id = bf.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据明细层' \n" +
            "\twhere instr(concat(',',bfe.VALUE,','),concat(',',bf.ename,',')) <> 0\n" +
            "GROUP BY\n" +
            "\tb.ename,bf.ename\n" +
            "\tORDER BY bf.FIELD_ID asc";



    //获取数据标准层所有表的字段信息
    public final static String ODS_TABLE_FIELD_INFO_SQL = "\tSELECT\n" +
            "\tt1.ENAME AS ENAME,\n" +
            "\t'数据标准层' AS `LEVEL`,\n" +
            "\tb.ENAME AS F_ENAME,\n" +
            "\tb.NAME AS F_CNAME,\n" +
            "\tIFNULL( SUM( t.dimension_value ), 0 ) AS F_COUNT,\n" +
            "\tb.FIELD_ID AS SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_FIELD_INFO b ON  t.dimension = CONCAT( '[\"_tgt\",\"04\",\"',b.Field_ID,'\"]')\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id " +
            "\t-- 下面一句可有可无\n" +
            "\tAND t1.ENAME LIKE '%ODS_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据标准层' \n" +
            "GROUP BY\n" +
            "\tt1.TEMPLATE_ID,b.ename";

    //数据明细层所有报表的字段信息
    public final static String DWD_TABLE_FIELD_INFO_SQL = "\tSELECT\n" +
            "\tt1.ENAME AS ENAME,\n" +
            "\t'数据明细层' AS `LEVEL`,\n" +
            "\tb.ENAME AS F_ENAME,\n" +
            "\tb.NAME AS F_CNAME,\n" +
            "\tIFNULL( SUM( t.dimension_value ), 0 ) AS F_COUNT,\n" +
            "\tb.FIELD_ID AS SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_FIELD_INFO b ON  t.dimension = CONCAT( '[\"_tgt\",\"04\",\"',b.Field_ID,'\"]')\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id  AND b.FIELD_CODE is not null \n" +
            "\t-- 下面一句可有可无\n" +
            "\tAND t1.ENAME LIKE '%DWD_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据明细层' \n" +
            "GROUP BY\n" +
            "\tt1.TEMPLATE_ID,b.ename";

    //数据汇总层所有表的字段信息
    public final static String DWS_TABLE_FIELD_INFO_SQL = "SELECT\n" +
            "\tt1.ENAME AS ENAME,\n" +
            "\t'数据汇总层' AS `LEVEL`,\n" +
            "\tb.ENAME AS F_ENAME,\n" +
            "\tb.NAME AS F_CNAME,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS F_COUNT,\n" +
            "\tb.FIELD_ID AS SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_FIELD_INFO b ON  t.dimension = CONCAT( '[\"_tgt\",\"04\",\"',b.Field_ID,'\"]')\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id  AND b.FIELD_CODE is not null \n" +
            "\t-- 下面一句可有可无\n" +
            "\tAND t1.ENAME LIKE '%DWS_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据汇总层' \n" +
            "GROUP BY\n" +
            "\tt1.TEMPLATE_ID,b.ename";

    //数据维度层所有表的字段信息
    public final static String DIM_TABLE_FIELD_INFO_SQL = "SELECT\n" +
            "\tt1.ENAME AS ENAME,\n" +
            "\t'数据维度层' AS `LEVEL`,\n" +
            "\tb.ENAME AS F_ENAME,\n" +
            "\tb.NAME AS F_CNAME,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS F_COUNT,\n" +
            "\tb.FIELD_ID AS SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_FIELD_INFO b ON  t.dimension = CONCAT( '[\"_tgt\",\"04\",\"',b.Field_ID,'\"]')\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id  AND b.FIELD_CODE is not null \n" +
            "\t-- 下面一句可有可无\n" +
            "\tAND t1.ENAME LIKE '%DIM_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据维度层' \n" +
            "GROUP BY\n" +
            "\tt1.TEMPLATE_ID,b.ename";

    //数据应用层所有表的字段信息
    public final static String ADM_TABLE_FIELD_INFO_SQL = "SELECT\n" +
            "\tt1.ENAME AS ENAME,\n" +
            "\t'数据应用层' AS `LEVEL`,\n" +
            "\tb.ENAME AS F_ENAME,\n" +
            "\tb.NAME AS F_CNAME,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS F_COUNT,\n" +
            "\tb.FIELD_ID AS SEQUENCE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN BASE_FIELD_INFO b ON  t.dimension = CONCAT( '[\"_tgt\",\"04\",\"',b.Field_ID,'\"]')\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id  AND b.FIELD_CODE is not null \n" +
            "\t-- 下面一句可有可无\n" +
            "\tAND t1.ENAME LIKE '%ADM_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据应用层' \n" +
            "GROUP BY\n" +
            "\tt1.TEMPLATE_ID,b.ename";


    //根据CRUST的数据量存储表来获取STG表的数据量
    public final static String STG_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据缓冲层'  AS LEVEL,\n" +
            "\tIFNULL( t.DATA_SIZE, 0 ) AS TABLE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tentity_quality_info t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON \n" +
            "\tt.ENTITY_ID = b.id \n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tAND b.ENAME LIKE '%STG_%'\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据缓冲层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    public final static String STG_ONE_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            " '数据缓冲层' AS LEVEL_NAME,"+
            "\tIFNULL( t.DATA_SIZE, 0 ) AS TABLE_NUM\n" +
            "FROM\n" +
            "\tentity_quality_info t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON \n" +
            "\tt.ENTITY_ID = b.id \n" +
            "\twhere b.ENAME = :tableName\n" +
            "GROUP BY\n" +
            "\tb.ename";



    //根据CRUST的数据量存储表来获取ODS表的数据量
    public final static String ODS_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据标准层'  AS LEVEL,\n" +
            "\tIFNULL( t.DATA_SIZE, 0 ) AS TABLE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tentity_quality_info t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON \n" +
            "\tt.ENTITY_ID = b.id \n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tAND b.ENAME LIKE '%ODS_%'\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据标准层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    public final static String ODS_ONE_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            " '数据标准层' AS LEVEL_NAME,"+
            "\tIFNULL( t.DATA_SIZE, 0 ) AS TABLE_NUM\n" +
            "FROM\n" +
            "\tentity_quality_info t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON \n" +
            "\tt.ENTITY_ID = b.id \n" +
            "\twhere b.ENAME = :tableName\n" +
            "GROUP BY\n" +
            "\tb.ename";


    //根据CRUST的数据量存储表来获取DWD表的数据量
    public final static String DWD_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据明细层'  AS LEVEL,\n" +
            "\tIFNULL( t.DATA_SIZE, 0 ) AS TABLE_NUM,\n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tentity_quality_info t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON \n" +
            "\tt.ENTITY_ID = b.id \n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tAND b.ENAME LIKE '%DWD_%'\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据明细层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    public final static String DWD_ONE_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            " '数据明细层' AS LEVEL_NAME,"+
            "\tIFNULL( t.DATA_SIZE, 0 ) AS TABLE_NUM\n" +
            "FROM\n" +
            "\tentity_quality_info t\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON \n" +
            "\tt.ENTITY_ID = b.id \n" +
            "\twhere b.ENAME = :tableName\n" +
            "GROUP BY\n" +
            "\tb.ename";


    //获取数据汇总层相关表信息
    public final static String DWS_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据汇总层'  AS LEVEL_NAME,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target_id = b.id" +
//            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension like '[\"_tgt\",\"01\"%'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id \n" +
            "\tAND b.ENAME LIKE '%DWS_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据汇总层' \n" +
            "GROUP BY\n" +
            "\tb.ename";

    public final static String DWS_ONE_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            " '数据汇总层' AS LEVEL_NAME,"+
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target_id = b.id" +
//            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension like '[\"_tgt\",\"01\"%'\n" +
            " AND t.target_id > 0" +
            "\twhere b.ENAME = :tableName\n" +
            "GROUP BY\n" +
            "\tb.ename";

    //获取数据维度层相关表信息
    public final static String DIM_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据维度层'  AS LEVEL_NAME,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target_id = b.id \n" +
//            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension like '[\"_tgt\",\"01\"%'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id \n" +
            "\tAND b.ENAME LIKE '%DIM_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据维度层' \n" +
            "GROUP BY\n" +
            "\tb.ename";


    public final static String DIM_ONE_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            " '数据维度层' AS LEVEL_NAME,"+
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM \n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target_id = b.id \n" +
//            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension like '[\"_tgt\",\"01\"%'\n" +
            " AND t.target_id > 0" +
            "\twhere b.ENAME = :tableName\n" +
            "GROUP BY\n" +
            "\tb.ename";


    //获取数据应用成相关表信息
    public final static String ADM_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            "\t'数据应用层'  AS LEVEL_NAME,\n" +
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM, \n" +
            "\t'' AS AREA,\n" +
            "\t'' AS AREA_ENAME,\n" +
            "\t'' AS ENTER_TIME\n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target_id = b.id \n" +
//            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension like '[\"_tgt\",\"01\"%'\n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id \n" +
            "\tAND b.ENAME LIKE '%ADM_%'\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id\n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = '数据应用层' \n" +
            "GROUP BY\n" +
            "\tb.ename\t";


    public final static String ADM_ONE_TABLE_QUALITY_SQL = "SELECT\n" +
            "\tb.ename AS ENAME,\n" +
            "\tb.NAME AS CNAME,\n" +
            " '数据应用层' AS LEVEL_NAME,"+
            "\tIFNULL( SUBSTR( MAX( CONCAT( t.TIMESTAMP, t.dimension_value ) ), 11 ), 0 ) AS TABLE_NUM \n" +
            "FROM\n" +
            "\tSTATISTICS_LOG t\n" +
            "\tRIGHT JOIN base_entity_info b ON t.target_id = b.id \n" +
//            "\tAND t.target_id = b.id \n" +
            "\tAND t.dimension like '[\"_tgt\",\"01\"%'\n" +
            " AND t.target_id > 0" +
            "\twhere b.ENAME = :tableName\n" +
            "GROUP BY\n" +
            "\tb.ename\t";


    public final static String STG_ODS_DWD_QUERY_FIELD_SQL = "\tSELECT\n" +
            "  IFNULL(JSON_EXTRACT(JSON_EXTRACT(f.quality_info,concat('$.\"',bf.field_id,'\"')),'$.\"correct\"'),0) AS fieldValuableRows,\n" +
            "\t-- b.ename AS ename,\n" +
            "\tbf.ename as fieldEnglishName,\n" +
            "\tbf.name as fieldChineseName\n" +
            "FROM\n" +
            "\tentity_field_quality_info f \n" +
            "\tRIGHT JOIN entity_quality_info t ON f.ENTITY_QUALITY_ID = t.id\n" +
            "\tRIGHT JOIN BASE_ENTITY_INFO b ON t.ENTITY_ID = b.id\n" +
            "\tRIGHT JOIN base_entity_extends bfe ON b.id = bfe.entity_id and bfe.key = 'column.name' \n" +
            "\tJOIN BASE_RESOURCE_TEMPLATE_INFO t1 ON b.template_id = t1.template_id\n" +
            "\tjoin base_field_info bf on b.template_id = bf.template_id\n" +
            "\tJOIN ( SELECT DISTINCT resource_id, dir_id FROM BASE_RESOURCE_DIR_RELATION ) t2 ON t1.TEMPLATE_ID = t2.resource_id \n" +
            "\tJOIN BASE_RESOURCE_DIR t3 ON t2.dir_id = t3.id \n" +
            "\tAND t3.NAME = :levelName \n" +
            "\twhere instr(concat(',',bfe.VALUE,','),concat(',',bf.ename,',')) <> 0 AND\n" +
            "\tb.ename = :tableName \n" +
            "GROUP BY\n" +
            "\tb.ename,bf.ename\n" +
            "\tORDER BY bf.FIELD_ID asc";

    public final static String EXIST_IN_SQLITE_CONFIG = "SELECT * FROM TABLE_OUT_JOB_REL " +
            "WHERE LEVEL_ENAME = :levelName AND TABLE_ENAME= :tableName";





}
