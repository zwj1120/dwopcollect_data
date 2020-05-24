package com.fh.daml.datawarehouse.operation.service;

import com.fh.daml.datawarehouse.operation.base.CheckResultTypeConstants;
import com.fh.daml.datawarehouse.operation.base.CommonConstants;
import com.fh.daml.datawarehouse.operation.dao.ToolDao;
import com.fh.daml.datawarehouse.operation.entity.TopicCheckItemEntity;
import com.fh.daml.datawarehouse.operation.entity.TopicCheckResultEntity;
import com.fh.daml.datawarehouse.operation.utils.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

/**
 * Author:zwj
 * Date:2020/1/14 16:19
 * Description:
 * Modified By:
 */
public class RelationThemeBase {

    private final static Logger logger = Logger.getLogger(RelationThemeBase.class);

    private static ToolDao toolDao = new ToolDao();


    private static Map<String, Object> relationType = new HashMap<>();

    static {
        relationType.clear();
        relationType.putAll(getRelationType());
    }


    public Boolean saveRelationCountToSqlite() throws Exception{
        try {
            toolDao.saveRelationCountToSqlite(getRelationCount("关系主题库"));
            return Boolean.TRUE;
        }catch (Exception e) {
            throw new Exception(e);
        }
    }

    private   static Map<String, Object> getRelationType() {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> typeList;
        try {
            typeList = SqliteJdbcTemplateUtil.getInstance().queryForListMap("SELECT DISTINCT RELATIONTYPE,SUMVALUE,PERCENTRANGE,PARENTTYPE,TYPEIGNORE,TYPEIGNOREREASON FROM RELATION_TOPIC_STATICS ");
            if (!typeList.isEmpty()) {
                for (Map<String, Object> map : typeList) {
                    result.put(String.valueOf(map.get("RELATIONTYPE")), SqliteJdbcTemplateUtil.getInstance().queryForList(String.class, "SELECT JOBID FROM  RELATION_TOPIC_STATICS WHERE RELATIONTYPE = '" + map.get("RELATIONTYPE") + "'"));
                }
                result.put("typeList", typeList);
            } else {
                logger.info("获取关系类型数为0");
            }
        } catch (SQLException e) {
            logger.error("获取关系类型SQL执行失败");
            e.printStackTrace();
        }


        List<Map<String, Object>> jobList = null;
        try {
            jobList = SqliteJdbcTemplateUtil.getInstance().queryForListMap("SELECT * FROM RELATION_TOPIC_STATICS ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (null == jobList || 0 == jobList.size()) {
            for (Map<String, Object> map : jobList) {
                result.put(String.valueOf(map.get("JOBID")), map);
            }
        }
        List<String> neverExecute = null;
        try {
            neverExecute = MysqlJdbcTemplateUtil.getInstance().queryForList(String.class, "SELECT JOB_ID FROM DTI_SCHEDULE_JOB WHERE JOB_ID NOT IN(SELECT DISTINCT JOB_ID FROM DTI_SCHEDULE_TASK)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        result.put("neverExecute", neverExecute);
        List<String> isStop = null;
        try {
            isStop = MysqlJdbcTemplateUtil.getInstance().queryForList(String.class, "SELECT JOB_ID FROM DTI_SCHEDULE_JOB WHERE JOB_STATE='3' ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        result.put("isStop", isStop);
        return result;
    }



    private List<Map<String, Object>> getRelationCount(String themeName) {
        List<Map<String, Object>> result = new ArrayList<>();
        // 读文件
        List<String> tempList = getThemeBaseAndRelationGraphStatistics(themeName);
        // sqlite表里，自己配好的关系
        List<Map<String, Object>> tempRelationList = (List<Map<String, Object>>) relationType.get("typeList");
        //获取该地全部的人数
        String sumValue = toolDao.getSumValue();
        // 查表遍历
        for (Map<String, Object> stringObjectMap : tempRelationList) {
            Map<String, Object> map = new HashMap<>();
            map.put("type", stringObjectMap.get("PARENTTYPE"));
            map.put("relation", stringObjectMap.get("RELATIONTYPE"));
            map.put("range", stringObjectMap.get("PERCENTRANGE"));
            map.put("sumValue", sumValue);
            map.put("ignore", stringObjectMap.get("TYPEIGNORE"));
            map.put("ignoreReason", stringObjectMap.get("TYPEIGNOREREASON"));

            // 读取关系量
            for (String tempString : tempList) {
                if (tempString.contains(String.valueOf(stringObjectMap.get("RELATIONTYPE")))) {
                    map.put("count", tempString.split("\u0016")[2]);
                }
            }
            String percent = CalculateUtils.getPerCents(String.valueOf(map.get("count")), sumValue).replace("%", "");
            if (String.valueOf(map.get("range")).contains(">")) {
                if (Double.parseDouble(percent) > Double.parseDouble(String.valueOf(map.get("range")).replace(">", ""))) {
                    map.put("isNormal", "正常");
                } else {
                    map.put("isNormal", "异常");
                }
            } else if (String.valueOf(map.get("range")).contains("-")) {
                if (Double.parseDouble(percent) > Double.parseDouble(String.valueOf(map.get("range")).split("-")[0])
                        && Double.parseDouble(percent) < Double.parseDouble(String.valueOf(map.get("range")).split("-")[1])
                        ) {
                    map.put("isNormal", "正常");
                } else {
                    map.put("isNormal", "异常");
                }
            }
            result.add(map);


        }
        return result;
    }

    private List<String> getThemeBaseAndRelationGraphStatistics(String themeBaseName) {

        try {
            List<String> result = new ArrayList<>();
            Map<String, TopicCheckResultEntity> resultEntityMap = topicCheckMain();
            //判空
            if (resultEntityMap == null || resultEntityMap.isEmpty()) {
                return Collections.emptyList();
            }
            //按themeBaseName处理
            for (Map.Entry<String, TopicCheckResultEntity> entry : resultEntityMap.entrySet()) {
                if (entry.getValue().getTopic().equals(themeBaseName)) {
                    result.add(entry.getValue().getTopic() + CommonConstants.SPLIT_CHAR +
                            entry.getValue().getItem() + CommonConstants.SPLIT_CHAR + entry.getValue().getCount() + CommonConstants.SPLIT_CHAR +
                            entry.getValue().getTableEName() + CommonConstants.SPLIT_CHAR + entry.getValue().getJobId());
                }
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }


    private Map<String, TopicCheckResultEntity> topicCheckMain() {
        Map<String, TopicCheckResultEntity> resultEntityMap = new HashMap<>();
        ArrayList<TopicCheckItemEntity> topicCheckItemEntityArrayList = new ArrayList<TopicCheckItemEntity>();
        String inputPath = "topicDataCount.input";
        String fileName = PathUtil.getRootPath(inputPath);
        if (StringUtils.isEmpty(fileName)) {
            return resultEntityMap;
        }
        try (InputStreamReader isr = new InputStreamReader(new FileInputStream(fileName), "UTF-8");
             BufferedReader reader = new BufferedReader(isr)) {
            String info;
//            读取每一行数据
            while ((info = reader.readLine()) != null) {
                TopicCheckItemEntity topicCheckItemEntity = readInput(info);
                if (CheckResultTypeConstants.TOOL_COUNT_TYPE.equals(topicCheckItemEntity.getActionType()) &&
                        CheckResultTypeConstants.TOOL_COUNT_JOB_TYPE.equals(topicCheckItemEntity.getCountType())) {
                    topicCheckItemEntityArrayList.add(topicCheckItemEntity);
                }
            }
            resultEntityMap = topicCheck(topicCheckItemEntityArrayList);
            return resultEntityMap;
        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("校验失败" + e.getMessage());
        }
        return resultEntityMap;
    }


    private TopicCheckItemEntity readInput(String info) throws Exception {
        TopicCheckItemEntity topicCheckItemEntity = new TopicCheckItemEntity();

        String[] inputArr = info.split("#");
        String topic = check(inputArr[0], "主题域").trim();
        //关系类型项
        String item = check(inputArr[1], "关系类型").trim();
        String strategy = check(inputArr[3], "执行策略").trim().toUpperCase();
        String actionType = check(inputArr[4], "操作类型").trim().toUpperCase();
        String countType = check(inputArr[5], "统计类型").trim().toUpperCase();
        if (!Arrays.asList(CommonConstants.DTI_JOB_EXECUTE_STRATEGY).contains(strategy)) {
            logger.error(info);
            throwMsg("请检查执行策略");
        }
        if (!Arrays.asList(CheckResultTypeConstants.TOOL_ACTION_TYPE).contains(actionType)) {
            logger.error(info);
            throwMsg("请检查操作类型");
        }
        if (CheckResultTypeConstants.TOOL_COUNT_TYPE.equals(actionType) && !Arrays.asList(CheckResultTypeConstants.TOOL_COUNT_DETAIL_TYPE).contains(countType)) {
            logger.error(info);
            throwMsg("请检查统计类型");
        }
        String jobId = "";

        jobId = check(inputArr[6], "JOB_ID").trim();

        String tableEName = inputArr[2];
        if (StringUtils.isEmpty(tableEName)) {
            Object outName = DtiSqlParseUtil.getTableNamesBySql(inputArr[6]);
            tableEName = String.valueOf(outName);
        }
        topicCheckItemEntity.setTopic(topic).setItem(item).setStrategy(strategy).setActionType(actionType).setCountType(countType).setJobId(jobId).setTableEName(tableEName);
        return topicCheckItemEntity;
    }

    /**
     * @Author ZZW
     * @Param [paramValue, paramName]
     * @Return java.lang.String
     * @Date 2019/10/22 18:48
     * @Description 异常类
     **/
    private String check(String paramValue, String paramName) throws Exception {
        if (StringUtils.isEmpty(paramValue)) {
            throw new Exception("关系域数据量统计中，" + paramName + "不能为空！\n" +
                    "请检查输入文件：input/relationDataCount.input。格式如下：" +
                    "关系类型#英文表名#执行策略：INC（增量）FUL（全量）#操作类型：COUNT（统计），CHECK（检测）#统计类型（根据job还是table统计）：JOB，TABLE#jobId");
        }
        return paramValue;
    }


    /**
     * @Author ZZW
     * @Param [msg]
     * @Return java.lang.String
     * @Date 2019/10/22 18:48
     * @Description 异常类
     **/
    private String throwMsg(String msg) throws Exception {
        throw new Exception("关系域数据量统计中，" + msg + "！\n" +
                "请检查输入文件：input/relationDataCount.input。格式如下：" +
                "关系类型#英文表名#执行策略：INC（增量）FUL（全量）#操作类型：COUNT（统计），CHECK（检测）#统计类型（根据job还是table统计）：JOB，TABLE#jobId");
    }


    /**
     * @Author ZZW
     * @Param [topicCheckItemEntityArrayList]
     * @Return java.util.Map<java.lang.String, com.nuts.fh.dwt.entity.check.TopicCheckResultEntity>
     * @Date 2019/10/22 18:43
     * @Description 返回主题库统计结果的map key：主题库+"\u0019"+主题库项目名  value：主题库的统计bean
     **/
    private Map<String, TopicCheckResultEntity> topicCheck(ArrayList<TopicCheckItemEntity> topicCheckItemEntityArrayList) {
        Map<String, TopicCheckResultEntity> resultEntityHashMap = new HashMap<>();
        for (TopicCheckItemEntity topicCheckItemEntity : topicCheckItemEntityArrayList) {
//            执行策略校验
            if (CommonConstants.DTI_JOB_INCREASE_STRATEGY.equals(topicCheckItemEntity.getStrategy())) {
                topicCheckItemEntity.setCount(getOutputCountByJobIdInc(topicCheckItemEntity.getJobId()));
            } else {
                topicCheckItemEntity.setCount(getOutputCountByJobIdFul(topicCheckItemEntity.getJobId()));
            }
            String key = topicCheckItemEntity.getTopic() + "\u0019" + topicCheckItemEntity.getItem();
//            校验数据量
            if (topicCheckItemEntity.getCount().compareTo(new BigDecimal(0)) < 0) {
                TopicCheckResultEntity topicCheckResultEntity = new TopicCheckResultEntity();
                topicCheckResultEntity.setTopic(topicCheckItemEntity.getTopic());
                topicCheckResultEntity.setItem(topicCheckItemEntity.getItem());
                topicCheckResultEntity.setJobId(topicCheckItemEntity.getJobId());
                topicCheckResultEntity.setTableEName(topicCheckItemEntity.getTableEName());//topicCheckResultEntity.setCount(BigDecimal.valueOf(1));
                topicCheckResultEntity.setCount(BigDecimal.valueOf(0));
                if (resultEntityHashMap.containsKey(key)) {
                    topicCheckResultEntity.setJobId(resultEntityHashMap.get(key).getJobId() + "," + topicCheckItemEntity.getJobId());
                    topicCheckResultEntity.setCount(resultEntityHashMap.get(key).getCount());
                }
                resultEntityHashMap.put(key, topicCheckResultEntity);
            } else {
                TopicCheckResultEntity topicCheckResultEntity = new TopicCheckResultEntity();
                topicCheckResultEntity.setTopic(topicCheckItemEntity.getTopic());
                topicCheckResultEntity.setItem(topicCheckItemEntity.getItem());
                topicCheckResultEntity.setCount(topicCheckItemEntity.getCount());
                topicCheckResultEntity.setJobId(topicCheckItemEntity.getJobId());
                topicCheckResultEntity.setTableEName(topicCheckItemEntity.getTableEName());
                if (resultEntityHashMap.containsKey(key)) {
                    topicCheckResultEntity.setJobId(resultEntityHashMap.get(key).getJobId() + "," + topicCheckItemEntity.getJobId());
                    topicCheckResultEntity.setCount(resultEntityHashMap.get(key).getCount().add(topicCheckItemEntity.getCount()));
                }
                resultEntityHashMap.put(key, topicCheckResultEntity);
            }
        }
        return resultEntityHashMap;
    }


    /**
     * @Author ZZW
     * @Param [jobId]
     * @Return java.math.BigDecimal
     * @Date 2019/10/22 18:45
     * @Description 通过JOB_ID获取增量任务输出数据量
     **/
    private BigDecimal getOutputCountByJobIdInc(String jobId) {
        if (StringUtils.isEmpty(jobId)) {
            return new BigDecimal(-1);
        }
        /*String sql = "SELECT SUM(t1.DIMENSION_VALUE) FROM STATISTICS_LOG t1 WHERE t1.DIMENSION = '[\""
                + CountTypeConstants.DIMENSION_OUT_SIGN + "\",\""
                + CountTypeConstants.ENTITY_COUNT_TYPE + "\"]' AND "
                + "t1.PROCESS_ID = '" + jobId + "'";*/
        String sql = "SELECT SUM(t1.DIMENSION_VALUE) FROM STATISTICS_LOG t1 WHERE t1.PROCESS_ID = '"+jobId+"'  " +
                "AND  t1.DIMENSION like '[\"_tgt\",\"01\"%' AND target_id > 0";
        List<String> countList = null;
        try {
            countList = MysqlJdbcTemplateUtil.getInstance().queryForList(String.class, sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if ((countList == null || countList.isEmpty()) || StringUtils.isEmpty(countList.get(0))) {
            return new BigDecimal(-1);
        }
        return new BigDecimal(countList.get(0));
    }


    /**
     * @Author ZZW
     * @Param [jobId]
     * @Return java.math.BigDecimal
     * @Date 2019/10/22 18:45
     * @Description 通过JOB_ID获取全量任务输出数据量
     **/
    private BigDecimal getOutputCountByJobIdFul(String jobId) {
        if (StringUtils.isEmpty(jobId)) {
            return new BigDecimal(-1);
        }
        /*String sql = "SELECT t1.DIMENSION_VALUE FROM STATISTICS_LOG t1 WHERE t1.DIMENSION = '[\"" + CountTypeConstants.DIMENSION_OUT_SIGN + "\",\"01\"]' AND " +
                "t1.PROCESS_ID = '" + jobId + "' ORDER BY t1.TIMESTAMP DESC LIMIT 1";*/
        String sql = "SELECT\n" +
                "\tt1.DIMENSION_VALUE \n" +
                "FROM\n" +
                "\tSTATISTICS_LOG t1 \n" +
                "WHERE\n" +
                "\tt1.PROCESS_ID = '"+jobId+"' \n" +
                "\tAND t1.DIMENSION LIKE '[\"_tgt\",\"01\"%' \n" +
                "\tAND target_id > 0 \n" +
                "ORDER BY\n" +
                "\tt1.TIMESTAMP DESC \n" +
                "\tLIMIT 1";
        List<String> countList = null;
        try {
            countList = MysqlJdbcTemplateUtil.getInstance().queryForList(String.class, sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if ((countList == null || countList.isEmpty()) || StringUtils.isEmpty(countList.get(0))) {
            return new BigDecimal(-1);
        }
        return new BigDecimal(countList.get(0));
    }

}
