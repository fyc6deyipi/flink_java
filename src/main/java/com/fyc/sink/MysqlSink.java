package com.fyc.sink;

import com.fyc.pojo.UserBehavior;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ResourceBundle;

public class MysqlSink extends RichSinkFunction<List<UserBehavior>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource=new BasicDataSource();
        connection=getConnection(dataSource);


        String sql="insert into user_behavior values (?,?,?,?,?)";
        this.ps=connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        System.out.println("colse");
        //关闭连接和释放资源

        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(List<UserBehavior> value, Context context) throws Exception {
        for (UserBehavior u:value) {
            ps.setString(1,u.getUser_id());
            ps.setString(2,u.getItem_id());
            ps.setString(3,u.getCategory_id());
            ps.setString(4,u.getBehavior());
            ps.setTimestamp(5,
                    new Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(u.getTs()).getTime()));
            ps.addBatch();
        }
        int[] nums = ps.executeBatch();

        System.out.println("成功了插入了" + nums.length + "行数据");

    }

    public static Connection getConnection(BasicDataSource dataSource) {

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
//            logger.error("-----------mysql get connection has exception , msg = " + e.getMessage());
            e.printStackTrace();
        }
        return con;
    }
}
