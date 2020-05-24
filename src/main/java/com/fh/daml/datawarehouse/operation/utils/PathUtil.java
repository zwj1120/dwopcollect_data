package com.fh.daml.datawarehouse.operation.utils;

import com.fh.daml.datawarehouse.operation.entity.Elements;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;


/**
 * Author:zwj
 * Date:2020/1/6 9:12
 * Description:
 * Modified By:
 */
public class PathUtil {

    private static final Logger logger = Logger.getLogger(PathUtil.class);

    public static String getRootPath(String path){
        if (StringUtils.isEmpty(path)){
            return null;
        }
        if (path.startsWith("/")){
            return path;
        }
        return PathUtil.getRootPath() + path;
    }
    /**
     * 此方法用于获取项目根路径
     * @return 返回根路径
     */
    public static String getRootPath() {
        String filePath = PathUtil.class.getClassLoader().getResource("").toString();
        String rootPath = filePath.substring(filePath.indexOf('/'));
        String os = System.getProperties().getProperty("os.name");
        if (os != null && os.startsWith("Windows") && rootPath.startsWith("/")){
            rootPath = rootPath.substring(1);
        }
        return rootPath;
    }

    /**
     * 获取配置文件信息
     * @param path 配置文件地址
     * @return 配置文件结果pro
     */
    public static Properties getProperties(String path) {
        try {
            String rootPath = getRootPath(path);
            Properties pro = new Properties();
            pro.load(new BufferedReader(new InputStreamReader(new FileInputStream(rootPath),"utf-8")));
            return pro;
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 获取字典内容
     * @param path 字典存放路径
     * @return
     */
    public static Map<String,String> getDic(String path) {
        File file = new File(getRootPath(path));
        try(
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader isr = new InputStreamReader(inputStream);
                BufferedReader bf = new BufferedReader(isr)) {
            //开始处理文件中内容

            Map<String,String> result = new HashMap<>();
            String line = bf.readLine();
            String[] strs;
            while(StringUtils.isNotEmpty(line)) {
                strs = line.split("\t",-1);
                if (2 != strs.length) {
                    logger.error("字典内容错误！");
                    return Collections.emptyMap();
                }
                result.put(strs[1],strs[0]);
                line = bf.readLine();
            }
            return result;
        }catch (Exception e) {
            logger.error(e.getMessage());
        }
        return Collections.emptyMap();
    }


    /**
     * 获取规则信息
     * @param path
     * @return
     */
    public static  Map<String,Map<String,List<Elements>>> getRule(String path) {
        File file = new File(getRootPath(path));
        try(
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader isr = new InputStreamReader(inputStream);
                BufferedReader bf = new BufferedReader(isr)) {
            Map<String,Map<String,List<Elements>>> result = new HashMap<>();
            String line = bf.readLine();
            String[] strs;
            int index = 0;
            while(StringUtils.isNotEmpty(line)) {
                if (index > 7) {
                    strs = line.split("#",-1);
                    if (3 != strs.length) {
                        logger.error("规则配置错误！");
                        return Collections.emptyMap();
                    }
                    if (result.containsKey(strs[0]+"\t"+strs[1])) {
                        result.get(strs[0] + "\t" + strs[1]).put(strs[2],null);
                    }else {
                        Map<String,List<Elements>> list = new HashMap<>();
                        list.put(strs[2],null);
                        result.put(strs[0]+"\t"+strs[1],list);
                    }
                }
                line = bf.readLine();
                index++;
            }
            return result;
        }catch (Exception e) {
            logger.error(e.getMessage());
        }
        return Collections.emptyMap();
    }


    /**
     *
     * @param path
     * @return
     */
    public static  Set<String> getElementNoCorrelation(String path) {
        String rootPath = getRootPath(path);
        if (StringUtils.isEmpty(rootPath)) {
            return Collections.emptySet();
        }
        File file = new File(rootPath);
        try(
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader isr = new InputStreamReader(inputStream);
                BufferedReader bf = new BufferedReader(isr)) {
            Set<String> result = new HashSet<>();
            String line = bf.readLine();
            String[] strs;
            while(StringUtils.isNotEmpty(line)) {
                strs = line.split("#",-1);
                if (2 != strs.length) {
                    logger.error("规则配置错误！");
                    return Collections.emptySet();
                }
                if (!result.contains(strs[0]+"\t"+strs[1])) {
                    result.add(strs[0]+"\t"+strs[1]);
                }
                line = bf.readLine();
            }
            return result;
        }catch (Exception e) {
            logger.error(e.getMessage());
        }
        return Collections.emptySet();
    }


    /**
     * 获取资源库要素的字典集
     * @param path
     * @return
     */
    public static Map<String,Map<String,String>> getElementDics(String path) {

        String rootPath = getRootPath(path);
        if (StringUtils.isEmpty(rootPath)) {
            return Collections.emptyMap();
        }
        File filesPath = new File(rootPath);
        if (filesPath.isDirectory()) {
            File[] files = filesPath.listFiles();
            if ( null != files && 0 != files.length) {
                Map<String,Map<String,String>> result = new HashMap<>();

                for (File file :files) {
                    String[] fileSplit = file.toPath().toString().split("\\\\", -1);
                    String dicName = fileSplit[fileSplit.length-1];
                    Map<String,String> dicMap = new HashMap<>();

                    try(
                            FileInputStream inputStream = new FileInputStream(file);
                            InputStreamReader isr = new InputStreamReader(inputStream);
                            BufferedReader bf = new BufferedReader(isr)) {
                        String line = bf.readLine();
                        String[] strs;
                        while(StringUtils.isNotEmpty(line)) {
                            strs = line.split("\t",-1);
                            if (2 != strs.length) {
                                logger.error(file.toPath().toString()+"字典格式错误！");
                                return Collections.emptyMap();
                            }
                            dicMap.put(strs[0],strs[1]);
                            line = bf.readLine();
                        }
                        result.put(dicName,dicMap);
                    }catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }
                return result;
            }
        }
        return Collections.emptyMap();
    }


}
