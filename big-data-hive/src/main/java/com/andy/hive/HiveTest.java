package com.andy.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * jdbc连接hive
 * @author leone
 * @since 2018-06-17 18:44
 **/
public class HiveTest {
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://node-1:10000/test", "hive", "");
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("select id, name, age from person");

        while (result.next()) {
            Integer id = result.getInt(1);
            String name = result.getString(2);
            Integer age = result.getInt(2);
            System.out.println("id:" + id + "name:" + name + "age:" + age);
        }

    }
}
