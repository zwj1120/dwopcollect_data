package com.fh.daml.datawarehouse.operation.service;

import com.fh.daml.datawarehouse.operation.entity.Elements;
import com.fh.daml.datawarehouse.operation.utils.PathUtil;
import com.fh.daml.datawarehouse.operation.utils.SqliteJdbcTemplateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * Author:zwj
 * Date:2020/1/15 13:44
 * Description:
 * Modified By:
 */
public class AdmElementDistributeStatistics {


    private final static Logger logger = Logger.getLogger(AdmElementDistributeStatistics.class);


    private static Map<String,String> dicMap = null;
    private static Map<String,Map<String,List<Elements>>> ruleMap = null;

    private static Set<String> needTopTenMap = null;

    //key-字段文件名 value-{key-文件第一个字段；value-文件第二个字段}
    private static Map<String,Map<String,String>> cdmElementDics = null;

    static {
        if (null == dicMap) {
            dicMap = PathUtil.getDic("dictionary.dic");
        }

        if (null == ruleMap) {
            ruleMap = PathUtil.getRule("rule.conf");
        }

        if (null == needTopTenMap) {
            needTopTenMap = PathUtil.getElementNoCorrelation("needTopTen.dic");
        }

        if (null == cdmElementDics) {
            cdmElementDics = PathUtil.getElementDics("dic");
        }
    }


    /**
     * 执行方法
     * @return
     */
    public Boolean execute() throws Exception{

        CdmElementDistributeStatistics cedvs = new CdmElementDistributeStatistics();
        //2.adm-person
        cedvs.execute("ADM_PERSON_FILE_PATH","person");
        //3.adm-relation
        cedvs.execute("ADM_RELATION_FILE_PATH","relation");
        Properties properties = PathUtil.getProperties("conf.properties");
        String conflictPath = properties.getProperty("CONFLICT_FILE_PATH");
        if (StringUtils.isEmpty(conflictPath)) {
            logger.error("文件地址为空！");
            return false;
        }
        executorConflict(conflictPath,"conflict");
        return true;
    }


    private static void executorConflict(String filePath,String typeName) throws Exception{
        String deleteSql = "delete from ADM_ELEMENT_DISTRIBUTE_STATISTICS where type='conflict'";
        if (SqliteJdbcTemplateUtil.getInstance().executeUpdate(deleteSql) < 0) {
            throw new Exception("删除Sqlite库中表BASE_ELEMENT_STATISTICS数据失败！");
        }

        File files = new File(filePath);
        if (files.isDirectory()) {
            File[] fileChildren = files.listFiles();
            handleFilesConflict(fileChildren,filePath,typeName);
        }else {
            logger.error("文件地址输入错误！");
        }
    }


    /**
     * 处理文件
     * @param files 文件列表
     * @param resultPath 结果路径
     */
    private static void handleFilesConflict(File[] files,String resultPath,String typeName) {
        if (null == files || 0 == files.length) {
            logger.error("路径下无文件！");
            return;
        }

        try {

            //循环处理子文件夹
            for (File f:files) {
                //只处理要素文件夹
                if (f.isDirectory()) {
                    if (null != f.listFiles()) {
                        Long count = 0L;
                        for (File fileChild:f.listFiles()) {
                            count = count + handleOneFile2(fileChild);
                        }
                        //写入文件
                        writeContent2(f.getPath().toString(),count);
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }


    private static void writeContent2(String resultPath,Long count)throws Exception {

        String insertSql = "insert into ADM_ELEMENT_DISTRIBUTE_STATISTICS values";
        StringBuilder sb = new StringBuilder();
        if ( resultPath.contains("multipleParent") ) {
            sb.append("('公民身份','公民身份','一人多父母','"+count+"','conflict'),");
        }else if ( resultPath.contains("multiplePartner") ) {
            sb.append("('公民身份','公民身份','一人多配偶','"+count+"','conflict'),");
        }else if ( resultPath.contains("noMarrHaveChild") ) {
            sb.append("('公民身份','公民身份','未婚有子女','"+count+"','conflict'),");
        }else if ( resultPath.contains("noMarrHavePartner") ) {
            sb.append("('公民身份','公民身份','未婚有配偶','"+count+"','conflict'),");
        }else if ( resultPath.contains("opposexinprison") ) {
            sb.append("('公民身份','公民身份','异性同监','"+count+"','conflict'),");
        }else if ( resultPath.contains("diffname") ) {
            sb.append("('公民身份','公民身份','父子女不同姓','"+count+"','conflict'),");
        }
        insertSql = insertSql + sb.deleteCharAt(sb.length()-1).toString();
        SqliteJdbcTemplateUtil.getInstance().executeUpdate(insertSql);
    }


    private static Long handleOneFile2(File file){

        if (null == dicMap) {
            logger.error("获取字典文件异常！");
        }
        Long count = 0L;
        try(FileInputStream inputStream = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(inputStream);
            BufferedReader bf = new BufferedReader(isr)) {
            String line = bf.readLine();
            while (StringUtils.isNotEmpty(line)) {
                count++;
                line = bf.readLine();
            }
        }catch (Exception e) {
            logger.error(e.getMessage());
        }

        return count;
    }


}
