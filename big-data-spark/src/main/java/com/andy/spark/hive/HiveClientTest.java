package com.andy.spark.hive;

import java.sql.*;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-13
 **/
public class HiveClientTest {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://node-1:10000/default");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from t_app_log");
//        ResultSet resultSet = statement.executeQuery("select * from t_boy order by fv desc");

//        System.out.println("id\tname\tfv");
        while (resultSet.next()) {
//            int id = resultSet.getInt("id");
//            String name = resultSet.getString("name");
//            int fv = resultSet.getInt("fv");
//            System.out.println(id + "\t" + name + "\t+" + fv);
            System.out.println(resultSet.getInt(1));
        }

        resultSet.close();
    }

}
