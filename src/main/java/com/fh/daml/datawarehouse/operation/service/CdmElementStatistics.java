package com.fh.daml.datawarehouse.operation.service;

import com.fh.daml.datawarehouse.operation.dao.ToolDao;
import com.fh.daml.datawarehouse.operation.entity.ElementReportEntity;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author:zwj
 * Date:2020/1/14 19:22
 * Description:
 * Modified By:
 */
public class CdmElementStatistics {

    private static ToolDao toolDao = new ToolDao();


    public Boolean saveBaseElementStatistics() {
        try {
            toolDao.saveBaseElementStatistics(getElementReport());
            return Boolean.TRUE;
        }catch (Exception e) {
            e.printStackTrace();
        }
        return Boolean.FALSE;
    }

    private ArrayList<ElementReportEntity> getElementReport(){
        ArrayList<ElementReportEntity> result = new ArrayList<ElementReportEntity>();
        try{
            List<Map<String, Object>> mapList =  toolDao.getAllElementsCount();
            String entityId = "-1";
            ElementReportEntity eleReport = new ElementReportEntity();
            for (Map<String, Object> element : mapList){
                String eleName = String.valueOf(element.get("element"));
                if (StringUtils.isEmpty(eleName)){
                    eleName = "unKnowField" + (String)element.get("field_id");
                }

                String count = String.valueOf(element.get("count"));
                String newEntityId = String.valueOf(element.get("id"));
                if ("-1".equals(entityId)){
                    eleReport.setEleName(eleName);
                    eleReport.setEleCount(count);
                    //更新最新的实体ID
                    entityId = newEntityId ;
                    continue;
                }
                if (!entityId.equals(newEntityId)){
                    result.add(eleReport);
                    eleReport = new ElementReportEntity();
                    eleReport.setEleName(eleName);
                    eleReport.setEleCount(count);
                    //更新最新的实体ID
                    entityId = newEntityId ;
                    continue;
                }
                ElementReportEntity relEleReport = new ElementReportEntity();
                relEleReport.setEleName(eleName.replace("数量",""));
                relEleReport.setEleCount(count);
                eleReport.getRelEleList().add(relEleReport);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

}
