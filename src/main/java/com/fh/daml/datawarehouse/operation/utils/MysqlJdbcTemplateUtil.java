package com.fh.daml.datawarehouse.operation.utils;

import com.gdk.jdbc.DBFactory;
import com.gdk.jdbc.JdbcHandler;

/**
 * Author:zwj
 * Date:2020/1/14 11:36
 * Description:
 * Modified By:
 */
public class MysqlJdbcTemplateUtil {
    public MysqlJdbcTemplateUtil() {

    }

    public static JdbcHandler jdbcHandler = null;

    public static JdbcHandler getInstance() {
        if (jdbcHandler == null) {
            jdbcHandler = DBFactory.create("MY_DB");
        }
        return jdbcHandler;
    }
}
