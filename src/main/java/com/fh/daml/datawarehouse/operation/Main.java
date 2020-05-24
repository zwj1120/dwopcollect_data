package com.fh.daml.datawarehouse.operation;

import com.fh.daml.datawarehouse.operation.service.*;
import com.fh.daml.datawarehouse.operation.utils.ConfUtil;
import com.fh.daml.datawarehouse.operation.utils.MysqlJdbcTemplateUtil;
import com.fh.daml.datawarehouse.operation.utils.SqliteJdbcTemplateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Author:zwj
 * Date:2020/1/14 11:36
 * Description:
 * Modified By:
 */
public class Main {


    //日志打印器
    private final static Logger logger = Logger.getLogger(Main.class);

    private final static CollectTableStatistics cts = new CollectTableStatistics();
    private final static PersonThemeBase ptb = new PersonThemeBase();
    private final static RelationThemeBase rtb = new RelationThemeBase();
    private final static CdmElementStatistics ces = new CdmElementStatistics();
    private final static CdmElementDistributeStatistics cedvs = new CdmElementDistributeStatistics();
    private final static AdmElementDistributeStatistics aeds = new AdmElementDistributeStatistics();

    static {
        String province = ConfUtil.pro.getProperty("PROVINCE");
        String city = ConfUtil.pro.getProperty("CITY");
        String province_code = ConfUtil.pro.getProperty("PROVINCE_CODE");
        String city_code = ConfUtil.pro.getProperty("CITY_CODE");
        String city_num = ConfUtil.pro.getProperty("CITY_NUM");

        if (StringUtils.isNotEmpty(province) && StringUtils.isNotEmpty(city)
                && StringUtils.isNotEmpty(province_code) && StringUtils.isNotEmpty(city_code)) {
            try {
                SqliteJdbcTemplateUtil.getInstance().executeUpdate("delete from config");
                SqliteJdbcTemplateUtil.getInstance()
                        .executeUpdate("insert into config values(null,'"+province+"','"+city+"','','','"+city_num+"','"+city_code+"','"+province_code+"')");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 主函数
     * @param args 无传参
     */
    public static void main(String[] args){

        logger.info("开始运行...");
        try {
            //=====================================================
            /*//1.先收集表数据量
            logger.info("开始获取表数据量...");
            if (!cts.collectData()) {
                logger.error("表数据量获取失败！");
                return;
            }
            logger.info("表数据量获取成功！！！");
            //=====================================================
            //2.获取人员主题库有值率情况
            logger.info("开始获取人员主题库字段有值率...");
            if (!ptb.savePersonFieldRate()){
                logger.error("人员主题库字段有值率获取失败！");
                return;
            }
            logger.info("人员主题库字段有值率获取成功！！！");
            //=====================================================
            //3.获取关系主题库各关系数据量情况
            logger.info("开始获取关系主题库各关系数据量情况...");
            if (!rtb.saveRelationCountToSqlite()) {
                logger.error("获取关系主题库各关系数据量情况失败！");
                return;
            }
            logger.info("获取关系主题库各关系数据量情况成功！！！");
            //=====================================================
            //4.获取资源库基础要素的统计信息
            logger.info("开始获取资源库基础要素的统计信息...");
            if (!ces.saveBaseElementStatistics()) {
                logger.error("获取关系主题库各关系数据量情况失败！");
                return;
            }
            logger.info("获取关系主题库各关系数据量情况成功！！！");*/
            //=====================================================
            //5.获取资源分布的统计信息
            logger.info("开始获取资源分布的统计信息...");
            if (!cedvs.execute("CDM_FILE_PATH","CDM")) {
                logger.error("获取资源分布的统计信息失败！");
                return;
            }
            logger.info("获取资源分布的统计信息成功！！！");
            //=====================================================
            //6.获取主题库分布的统计信息
            logger.info("开始获取主题库分布的统计信息...");
            if (!aeds.execute()) {
                logger.error("获取主题库分布的统计信息失败！");
                return;
            }
            logger.info("获取主题库分布的统计信息成功！！！");

            logger.info("运行完成！！！");
        }catch (Exception e) {
            e.printStackTrace();
            logger.error("数据采集工具，失败结束！");
        }
    }

}
