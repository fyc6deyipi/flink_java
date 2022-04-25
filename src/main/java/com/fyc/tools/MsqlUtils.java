package com.fyc.tools;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.util.ResourceBundle;

public class MsqlUtils {


    public static   Connection getConnection() {
        BasicDataSource dataSource = new BasicDataSource();
        ResourceBundle jdbc_rb = ResourceBundle.getBundle("mysql");

        String mysql_jdbc_driver = jdbc_rb.getString("jdbc.driverClassName");
        String mysql_jdbc_url = jdbc_rb.getString("jdbc.url");
        String mysql_jdbc_username = jdbc_rb.getString("jdbc.username");
        String mysql_jdbc_password = jdbc_rb.getString("jdbc.password");
        dataSource.setDriverClassName(mysql_jdbc_driver);
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl(mysql_jdbc_url);
        dataSource.setUsername(mysql_jdbc_username);
        dataSource.setPassword(mysql_jdbc_password);
        //设置连接池的一些参数
        dataSource.setInitialSize(2);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);

        } catch (Exception e) {
            //logger.error("-----------mysql get connection has exception , msg = " + e.getMessage());
            e.printStackTrace();
        }
        return con;
    }
}
