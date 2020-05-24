package com.fh.daml.datawarehouse.operation.service;

import com.fh.daml.datawarehouse.operation.base.CommonConstants;
import com.fh.daml.datawarehouse.operation.dao.ToolDao;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Author:zwj
 * Date:2020/1/14 14:18
 * Description:
 * Modified By:
 */
public class CollectTableStatistics {


    private static ToolDao toolDao = new ToolDao();

    private static final Logger logger = Logger.getLogger(CollectTableStatistics.class);


    //开始收集数据
    public Boolean collectData() throws Exception {

        try {
            //搜集数据前需要先将本周的数据给清空，目的：只搜集每周最新的一次
            toolDao.deleteWeekData();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String format = sdf.format(new Date());

            //收集各数据层级表信息
            toolDao.importDataLevelInfo(CommonConstants.STG_NAME,format);
            toolDao.importDataLevelInfo(CommonConstants.ODS_NAME,format);
            toolDao.importDataLevelInfo(CommonConstants.DWD_NAME,format);
            toolDao.importDataLevelInfo(CommonConstants.DWS_NAME,format);
            toolDao.importDataLevelInfo(CommonConstants.DIM_NAME,format);
            toolDao.importDataLevelInfo(CommonConstants.ADM_NAME,format);
//            StatisticsDao statisticsDao = new StatisticsDao();
            //收集各数据层级表字段信息
            toolDao.importTableFieldInfo(CommonConstants.STG_NAME,format);
            toolDao.importTableFieldInfo(CommonConstants.ODS_NAME,format);
            toolDao.importTableFieldInfo(CommonConstants.DWD_NAME,format);
            toolDao.importTableFieldInfo(CommonConstants.DWS_NAME,format);
            toolDao.importTableFieldInfo(CommonConstants.DIM_NAME,format);
            toolDao.importTableFieldInfo(CommonConstants.ADM_NAME,format);
            return Boolean.TRUE;
        }catch (Exception e) {
            throw new Exception(e);
        }
    }

}
