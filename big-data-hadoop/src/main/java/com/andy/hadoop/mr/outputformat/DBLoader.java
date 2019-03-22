package com.andy.hadoop.mr.outputformat;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-22
 **/
public class DBLoader {

    public static void dbLoader(Map<String, String> map) {
        Connection conn = null;
        Statement st = null;
        ResultSet res = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://39.108.125.41:3306/boot?useSSL=false", "root", "cloud");
            st = conn.createStatement();
            res = st.executeQuery("select url,content from t_url_rule");
            while (res.next()) {
                map.put(res.getString(1), res.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (res != null) {
                    res.close();
                }
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<>();
        dbLoader(map);
        System.out.println(map.size());
    }


}
