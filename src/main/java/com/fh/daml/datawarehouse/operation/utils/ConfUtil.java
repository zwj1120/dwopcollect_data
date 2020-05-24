package com.fh.daml.datawarehouse.operation.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Author:zwj
 * Date:2020/1/14 14:39
 * Description:
 * Modified By:
 */
public class ConfUtil {

    public static Properties pro = null;
    private static String fileName = PathUtil.getRootPath("conf.properties");

    static {
        if (pro == null){
            pro = ConfUtil.loadConf();
        }
    }

    private static Properties loadConf(){
        Properties pro = new Properties();

        try{
            pro.load(new BufferedReader(new InputStreamReader(new FileInputStream(fileName),"utf-8")));
            return pro;
        }catch (IOException ioe){
            ioe.printStackTrace();
        }
        return null;
    }
}
