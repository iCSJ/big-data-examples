package com.andy.hive;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * <p> jdbc连接hive
 *
 * @author leone
 * @since 2018-06-17
 **/
public class HiveClientTest {


    private Connection connection;

    private Statement statement;

    @Before
    public void init() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection("jdbc:hive2://node-1:10000/db1", "root", "");
        statement = connection.createStatement();
    }

    /**
     * 查询表数据
     *
     * @throws Exception
     */
    @Test
    public void selectTest() throws Exception {
        ResultSet result = statement.executeQuery("select * from t_log");
        System.out.println("-----------------------------------------------------");
        System.out.println("id\tip\turl\ttime");
        System.out.println("-----------------------------------------------------");
        while (result.next()) {
            int id = result.getInt(1);
            String ip = result.getString(2);
            String url = result.getString(3);
            String time = result.getString(4);
            System.out.println("|" + id + "\t" + ip + "\t" + url + "\t" + time + "|");
        }
        System.out.println("-----------------------------------------------------");
    }


    /**
     * 创建表
     *
     * @throws Exception
     */
    @Test
    public void dropTableTest() throws Exception {
        String sql = "drop table if exists t_log";
        boolean flag = statement.execute(sql);
        System.out.println("running sql:" + sql + (flag ? " " : "failed success"));
    }


    /**
     * 删除数据库
     *
     * @throws Exception
     */
    @Test
    public void dropDatabaseTest() throws Exception {
        String sql = "drop database if exists db1";
        boolean flag = statement.execute(sql);
        System.out.println("running sql:" + sql + (flag ? " " : "failed success"));
    }


    /**
     * 创建表
     *
     * @throws Exception
     */
    @Test
    public void createTableTest() throws Exception {
        String sql = "create table t_log(id int, ip string, url string, time string) row format delimited fields terminated by ','";
        boolean flag = statement.execute(sql);
        System.out.println("running sql:" + sql + (flag ? " " : "failed success"));
    }


    /**
     * 查询所有表
     *
     * @throws Exception
     */
    @Test
    public void showTablesTest() throws Exception {
        ResultSet resultSet = statement.executeQuery("show tables");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }


    /**
     * 加载数据
     *
     * @throws Exception
     */
    @Test
    public void loadDataTest() throws Exception {
        String filePath = "/root/app.log";

        String sql = "load data local inpath '" + filePath + "' overwrite into table t_log";
        boolean flag = statement.execute(sql);
        System.out.println("running sql:" + sql + (flag ? " " : "failed success"));
    }


    /**
     * 统计查询（会运行 mapReduce 作业）
     *
     * @throws Exception
     */
    @Test
    public void countDataTest() throws Exception {
        String sql = "select count(1) from t_log";
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getInt(1));
        }
        boolean flag = statement.execute(sql);
        System.out.println("running sql:" + sql + (flag ? " " : "failed success"));
    }


}
