package com.andy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

/**
 * <p> HBase crud
 *
 * @author leone
 * @since 2018-12-16
 **/
public class HBaseClientTest {


    private Connection conn;

    @Before
    public void init() throws Exception {
        // 创建连接对象,会自动加载HBase配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node-2:2181,node-3:2181,node-4:2181");
        conn = ConnectionFactory.createConnection(conf);
    }


    /**
     * DDL操作
     *
     * @throws Exception
     */
    @Test
    public void createTableTest() throws Exception {
        // 创建ddl描述对象
        Admin admin = conn.getAdmin();

        // 创建表描述对象
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));

        // 创建列簇描述对象
        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("base_info");
        // 设置保存数据的最大半本数量是3
        hColumnDescriptor1.setMaxVersions(3);

        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("extra_info");


        hTableDescriptor.addFamily(hColumnDescriptor1);
        hTableDescriptor.addFamily(hColumnDescriptor2);

        admin.createTable(hTableDescriptor);

        admin.close();
        conn.close();
    }


    /**
     * DML 删除表
     */
    @Test
    public void dropTableTest() throws Exception {
        // 创建ddl描述对象
        Admin admin = conn.getAdmin();

        // 先停用表
        admin.disableTable(TableName.valueOf("user_info"));

        // 再删除表
        admin.deleteTable(TableName.valueOf("user_info"));

        admin.close();
        conn.close();

    }


    @Test
    public void alterTableTest() throws Exception {
        // 创建ddl描述对象
        Admin admin = conn.getAdmin();

        // 取出旧的的表的描述信息
        HTableDescriptor user_info = admin.getTableDescriptor(TableName.valueOf("user_info"));

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
        // 设置布隆过滤器
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);

        user_info.addFamily(hColumnDescriptor);

        admin.modifyTable(TableName.valueOf("user_info"), user_info);

        admin.close();
        conn.close();
    }


    /**
     * DML 操作HBase测试
     *
     * @throws Exception
     */
    @Test
    public void insertTest() throws Exception {
        // 创建ddl描述对象
        Admin admin = conn.getAdmin();

        // 取出旧的的表的描述信息
        HTableDescriptor user_info = admin.getTableDescriptor(TableName.valueOf("user_info"));

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
        // 设置布隆过滤器
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);

        user_info.addFamily(hColumnDescriptor);

        admin.modifyTable(TableName.valueOf("user_info"), user_info);

        admin.close();
        conn.close();
    }


}
