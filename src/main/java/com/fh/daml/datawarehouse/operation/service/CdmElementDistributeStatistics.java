package com.fh.daml.datawarehouse.operation.service;

import com.fh.daml.datawarehouse.operation.entity.Elements;
import com.fh.daml.datawarehouse.operation.utils.PathUtil;
import com.fh.daml.datawarehouse.operation.utils.SqliteJdbcTemplateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

/**
 * Author:zwj
 * Date:2020/1/15 13:44
 * Description:
 * Modified By:
 */
public class CdmElementDistributeStatistics {


    private final static Logger logger = Logger.getLogger(CdmElementDistributeStatistics.class);


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
    public Boolean execute(String filePathConfig,String type) throws Exception{
        //1.获取配置文件
        Properties properties = PathUtil.getProperties("conf.properties");
        if (properties == null) {
            logger.error("配置文件错误！");
            return Boolean.FALSE;
        }
        //2.获取目标文件存放地址
        String filePath = properties.getProperty(filePathConfig);
        if (StringUtils.isEmpty(filePath)) {
            logger.error("文件地址为空！");
            return Boolean.FALSE;
        }
        //3.对目标文件进行读取
        File files = new File(filePath);
        if (files.isDirectory()) {
            File[] fileChildren = files.listFiles();
            handleFiles(fileChildren,type);
        }else {
            logger.error("文件地址输入错误！");
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * 处理文件
     * @param files 文件列表
     */
    private void handleFiles(File[] files,String type) throws Exception{

        if (null == files || 0 == files.length) {
            logger.error("路径下无文件！");
            return;
        }

        if ("CDM".equals(type)) {
            String deleteSql = "delete from ELEMENT_DISTRIBUTE_STATISTICS";
            if (SqliteJdbcTemplateUtil.getInstance().executeUpdate(deleteSql) < 0) {
                throw new Exception("删除Sqlite库中表BASE_ELEMENT_STATISTICS数据失败！");
            }
        }else {
            String deleteSql = "delete from ADM_ELEMENT_DISTRIBUTE_STATISTICS where type='"+type+"'";
            if (SqliteJdbcTemplateUtil.getInstance().executeUpdate(deleteSql) < 0) {
                throw new Exception("删除Sqlite库中表BASE_ELEMENT_STATISTICS数据失败！");
            }
        }

        try {

            //循环处理子文件夹
            for (File f:files) {
                //只处理要素文件夹
                if (f.isDirectory()) {
                    if (null != f.listFiles()) {
                        Map<String,List<Elements>> map = new HashMap<>();
                        for (File fileChild:f.listFiles()) {
                            //处理单个文件
                            handleOneFile(fileChild,map);
                        }
                        //写入文件
                        writeContent(map,type);
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }


    /**
     * 处理单个文件
     * @param file file
     * @param map map
     */
    private void handleOneFile(File file,Map<String,List<Elements>> map){

        if (null == dicMap) {
            logger.error("获取字典文件异常！");
        }

        try(FileInputStream inputStream = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(inputStream);
            BufferedReader bf = new BufferedReader(isr)) {
            String line = bf.readLine();
            while (StringUtils.isNotEmpty(line)) {
                handleOneLine(line,map);
                line = bf.readLine();
            }
        }catch (Exception e) {
            logger.error(e.getMessage());
        }
    }


    /**
     * 处理文件中单行数据
     * @param line line
     * @param map map
     * @return
     * @throws Exception
     */
    private Map<String,List<Elements>> handleOneLine(String line, Map<String,List<Elements>> map) throws Exception{
        String[] strings1 = line.split("\t",-1);
        if (2 != strings1.length) {
            return map;
        }
        String[] strings2 = strings1[0].split("#",-1);
        if ( 4 != strings2.length) {
            return map;
        }
        String element1 =strings2[1];
        String element2 =strings2[2];
        String count1 = strings2[3];
        String count2 = strings1[1];
        Elements elements = new Elements(element1,element2,count1,count2);
        if (map.containsKey(element1+"\t"+element2)) {
            map.get(element1+"\t"+element2).add(elements);
        }else {
            List<Elements> list = new ArrayList<>();
            list.add(elements);
            map.put(element1+"\t"+element2,list);
        }
        return map;
    }


    /**
     * 输出内容
     * @param map map
     * @throws Exception Exception
     */
    private static void writeContent(Map<String,List<Elements>> map,String type)throws Exception {
        if (null == map) {
            return;
        }
        //排序和做限定
        Map<String, List<Elements>> stringListMap = sortAndLimitMap(map);

        for (Map.Entry<String,List<Elements>> entry:stringListMap.entrySet()) {

            List<Elements> value = entry.getValue();

            if (needTopTenMap.contains(entry.getKey())) {
                Collections.sort(value, new Comparator<Elements>() {
                    @Override
                    public int compare(Elements o1, Elements o2) {
                        return Integer.parseInt(String.valueOf(Long.parseLong(o2.getCountTwo()) - Long.parseLong(o1.getCountTwo())));
                    }
                });

                if (value.size() >= 10) {
                    value = value.subList(0,10);
                }

            }

            if (type.equals("CDM")) {

                String insertSql = "insert into ELEMENT_DISTRIBUTE_STATISTICS values";
                StringBuilder sb = new StringBuilder();
                for (Elements elements:value) {
                    if (null != elements) {
                        if (null == dicMap.get(elements.getElementOne())) {
                            logger.error(elements.getElementOne()+"找不到字典！");
                            continue;
                        }

                        if (null == dicMap.get(elements.getElementTwo())) {
                            logger.error(elements.getElementTwo()+"找不到字典！");
                            continue;
                        }
                        sb.append("('"+dicMap.get(elements.getElementOne())+"','"
                                +dicMap.get(elements.getElementTwo())+"','"
                                +elements.getCountOne()+"','"+elements.getCountTwo()+"'),");
                    }

                }
                insertSql = insertSql + sb.deleteCharAt(sb.length()-1).toString();
                SqliteJdbcTemplateUtil.getInstance().executeUpdate(insertSql);
            }else {

                String insertSql = "insert into ADM_ELEMENT_DISTRIBUTE_STATISTICS values";
                StringBuilder sb = new StringBuilder();
                for (Elements elements:value) {
                    if (null != elements) {
                        if (null == dicMap.get(elements.getElementOne())) {
                            logger.error(elements.getElementOne()+"找不到字典！");
                            continue;
                        }

                        if (null == dicMap.get(elements.getElementTwo())) {
                            logger.error(elements.getElementTwo()+"找不到字典！");
                            continue;
                        }
                        sb.append("('"+dicMap.get(elements.getElementOne())+"','"
                                +dicMap.get(elements.getElementTwo())+"','"
                                +elements.getCountOne()+"','"+elements.getCountTwo()+"','"+type+"'),");
                    }

                }
                try {
                    insertSql = insertSql + sb.deleteCharAt(sb.length()-1).toString();
                    SqliteJdbcTemplateUtil.getInstance().executeUpdate(insertSql);
                }catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }


    /**
     * 对一个文件的结果集进行排序和限定
     * @param map map
     */
    private static Map<String,List<Elements>> sortAndLimitMap(Map<String,List<Elements>> map) {
        if (null == map || 0 == map.size() || null == ruleMap ) {
            return null;
        }
        Map<String,List<Elements>> mapChild = new HashMap<>();
        for (Map.Entry<String,List<Elements>> entry:map.entrySet()) {
            //先进行排序
            try {
                Collections.sort(entry.getValue(), new Comparator<Elements>() {
                    @Override
                    public int compare(Elements o1, Elements o2) {
                        try {
                            return Integer.parseInt(o1.getCountOne())-Integer.parseInt(o2.getCountOne());
                        }catch (NumberFormatException e) {
                            return o1.getCountOne().compareTo(o2.getCountOne());
                        }
                    }
                });
            }catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            if (ruleMap.containsKey(entry.getKey())) {

                //根据规则进行处理
                Map<String,Elements> res = new HashMap<>();
                for (Elements elements:entry.getValue()) {
                    if (!isInclude(elements,entry.getKey(),res)) {
                        logger.error("请检查规则配置！");
                        return Collections.emptyMap();
                    }
                }

                //进行排序
                List<Elements> list1 = new ArrayList<Elements>(res.values());
                Collections.sort(list1, new Comparator<Elements>() {
                    @Override
                    public int compare(Elements o1, Elements o2) {
                        String num1 = o1.getCountOne();
                        if (num1.contains("-") ) {
                            num1 = o1.getCountOne().split("-",-1)[0];
                        }
                        if (num1.contains(">") ) {
                            num1 = o1.getCountOne().split(">",-1)[1];
                        }
                        if (num1.contains(">=") ) {
                            num1 = o1.getCountOne().split(">=",-1)[1];
                        }
                        if (num1.contains("<") ) {
                            num1 = o1.getCountOne().split("<",-1)[1];
                        }
                        if (num1.contains("<=") ) {
                            num1 = o1.getCountOne().split("<=",-1)[1];
                        }

                        String num2 = o2.getCountOne();
                        if (num2.contains("-") ) {
                            num2 = o2.getCountOne().split("-",-1)[0];
                        }
                        if (num2.contains(">") ) {
                            num2 = o2.getCountOne().split(">",-1)[1];
                        }
                        if (num2.contains(">=") ) {
                            num2 = o2.getCountOne().split(">=",-1)[1];
                        }
                        if (num2.contains("<") ) {
                            num2 = o2.getCountOne().split("<",-1)[1];
                        }
                        if (num2.contains("<=") ) {
                            num2 = o2.getCountOne().split("<=",-1)[1];
                        }
                        try {
                            return Integer.parseInt(num2)-Integer.parseInt(num1);
                        }catch (NumberFormatException e) {
                            return num2.compareTo(num1);
                        }
                    }
                });
                mapChild.put(entry.getKey(),list1);

            }else {
                logger.error(entry.getKey()+"无配置规则");
            }
        }
        return mapChild;
    }


    /**
     * 根据规则返回是否在规则内
     * @param index
     * @param key
     * @param res
     * @return
     */
    private static Boolean isInclude(Elements index,String key,Map<String,Elements> res) {
        try {

            if (null == ruleMap) {
                logger.error("获取规则信息失败！");
                return Boolean.FALSE;
            }
            Map<String,List<Elements>> rules;
            if (ruleMap.containsKey(key)) {
                rules = ruleMap.get(key);

                for (Map.Entry<String,List<Elements>> entry:rules.entrySet()) {

                    //无规则原样输出
                    if ("false".equals(entry.getKey())) {
                        if (res.containsKey(entry.getKey())) {
                            res.get(entry.getKey()).setCountTwo(String.valueOf(Long.parseLong(res.get(entry.getKey()).getCountTwo())
                                    +Long.parseLong(index.getCountTwo())));
                            return Boolean.TRUE;
                        }else {
                            res.put(entry.getKey()+index.getCountOne(),index);
                            return Boolean.TRUE;
                        }
                    }


                    if (entry.getKey().contains("-")) {
                        if (2 == entry.getKey().split("-",-1).length){
                            String num1 = entry.getKey().split("-",-1)[0];
                            String num2 = entry.getKey().split("-",-1)[1];
                            if (Long.parseLong(num1) <= Long.parseLong(index.getCountOne())
                                    && Long.parseLong(index.getCountOne()) <= Long.parseLong(num2)) {
                                if (res.containsKey(entry.getKey())) {
                                    res.get(entry.getKey()).setCountTwo(String.valueOf(Long.parseLong(res.get(entry.getKey()).getCountTwo())
                                            +Long.parseLong(index.getCountTwo())));
                                }else {
                                    index.setCountOne(entry.getKey());
                                    res.put(entry.getKey(),index);
                                }
                                return Boolean.TRUE;
                            }
                        }
                    }

                    if (entry.getKey().contains(">")) {
                        if (2 == entry.getKey().split(">",-1).length){
                            String num = entry.getKey().split(">",-1)[1];
                            if (Long.parseLong(num) < Long.parseLong(index.getCountOne())) {
                                if (res.containsKey(entry.getKey())) {
                                    res.get(entry.getKey()).setCountTwo(String.valueOf(Long.parseLong(res.get(entry.getKey()).getCountTwo())
                                            +Long.parseLong(index.getCountTwo())));
                                }else {
                                    index.setCountOne(entry.getKey());
                                    res.put(entry.getKey(),index);
                                }
                                return Boolean.TRUE;
                            }
                        }
                    }

                    if (entry.getKey().contains(">=")) {

                        if (2 == entry.getKey().split(">=",-1).length){
                            String num = entry.getKey().split(">=",-1)[1];
                            if (Long.parseLong(num) <= Long.parseLong(index.getCountOne())) {
                                if (res.containsKey(entry.getKey())) {
                                    res.get(entry.getKey()).setCountTwo(String.valueOf(Long.parseLong(res.get(entry.getKey()).getCountTwo())
                                            +Long.parseLong(index.getCountTwo())));
                                }else {
                                    index.setCountOne(entry.getKey());
                                    res.put(entry.getKey(),index);
                                }
                                return Boolean.TRUE;
                            }
                        }
                    }

                    if (entry.getKey().contains("<")) {
                        if (2 == entry.getKey().split("<",-1).length){
                            String num = entry.getKey().split("<",-1)[1];
                            if (Long.parseLong(num) > Long.parseLong(index.getCountOne())) {
                                if (res.containsKey(entry.getKey())) {
                                    res.get(entry.getKey()).setCountTwo(String.valueOf(Long.parseLong(res.get(entry.getKey()).getCountTwo())
                                            +Long.parseLong(index.getCountTwo())));
                                }else {
                                    index.setCountOne(entry.getKey());
                                    res.put(entry.getKey(),index);
                                }
                                return Boolean.TRUE;
                            }
                        }
                    }

                    if (entry.getKey().contains("<=")) {
                        if (2 == entry.getKey().split("<=",-1).length){
                            String num = entry.getKey().split("<=",-1)[1];
                            if (Long.parseLong(num) >= Long.parseLong(index.getCountOne())) {
                                if (res.containsKey(entry.getKey())) {
                                    res.get(entry.getKey()).setCountTwo(String.valueOf(Long.parseLong(res.get(entry.getKey()).getCountTwo())
                                            +Long.parseLong(index.getCountTwo())));
                                }else {
                                    index.setCountOne(entry.getKey());
                                    res.put(entry.getKey(),index);
                                }
                                return Boolean.TRUE;
                            }
                        }

                    }


                    //处理需要字典的情况
                    if (cdmElementDics.containsKey(entry.getKey())) {
                        if (res.containsKey(entry.getKey())) {
                            res.get(entry.getKey()).setCountTwo(String.valueOf(Long.parseLong(res.get(entry.getKey()).getCountTwo())
                                    +Long.parseLong(index.getCountTwo())));
                            return Boolean.TRUE;
                        }else {

                            String s = cdmElementDics.get(entry.getKey()).get(index.getCountOne());
                            if (StringUtils.isEmpty(s)) {
                                //无法匹配字典原样输出
                                //logger.error("无字典匹配："+index.toString());
                                return Boolean.TRUE;
                            }else {
                                index.setCountOne(s);
                            }
                            res.put(entry.getKey()+index.getCountOne(),index);
                            return Boolean.TRUE;


                        }
                    }

                }
                res.put(index.getCountOne(),index);
            }else {
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error(e.getMessage());
            return Boolean.TRUE;
        }
    }

}
