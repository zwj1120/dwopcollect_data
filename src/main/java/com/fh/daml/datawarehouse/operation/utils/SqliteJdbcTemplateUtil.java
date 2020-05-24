package com.fh.daml.datawarehouse.operation.utils;
import com.gdk.jdbc.DBFactory;
import com.gdk.jdbc.JdbcHandler;


/**
 * Author:zwj
 * Date:2020/1/14 11:36
 * Description:
 * Modified By:
 */
public class SqliteJdbcTemplateUtil {
      public SqliteJdbcTemplateUtil(){

      }
    private static JdbcHandler jdbcHandler = null;
    public static JdbcHandler getInstance(){
        if(jdbcHandler == null){
            String path = MysqlJdbcTemplateUtil.class.getClassLoader().getResource("tools.db").toString();
            try{
                DBFactory.shutdown("sqliteJdbc");
                jdbcHandler = DBFactory.create("sqliteJdbc", "org.sqlite.JDBC",
                       "jdbc:sqlite:"+path, "", "");
           }catch (Exception  e){
                e.printStackTrace();
            }
        }
        return jdbcHandler;
    }
}
