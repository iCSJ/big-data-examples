package com.andy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

/**
 * <p> Hbase crud
 *
 * @author leone
 * @since 2018-12-16
 **/
public class HbaseClientDemo {


    @Test
    public void testCreateTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("base_info");
        hColumnDescriptor.setMaxVersions(3);

        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("extra_info");


        hTableDescriptor.addFamily(hColumnDescriptor);
        hTableDescriptor.addFamily(hColumnDescriptor1);

        admin.createTable(hTableDescriptor);

        admin.close();
    }

}
