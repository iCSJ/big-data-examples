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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> HBase crud
 *
 * @author leone
 * @since 2018-12-16
 **/
public class HBaseClientTest {


    private static final Logger logger = LoggerFactory.getLogger(HBaseCrudTest.class);

    private Connection conn;

    private Admin admin;

    private String tableName = "t_person";

    private String f1 = "f1";

    private String f2 = "f2";

    private String f3 = "f3";

    @Before
    public void init() throws Exception {
        // 创建连接对象,会自动加载HBase配置文件 zookeeper集群的URL配置信息
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node-2:2181,node-3:2181,node-4:2181");
        conn = ConnectionFactory.createConnection(conf);
        // 创建ddl描述对象
        admin = conn.getAdmin();
    }

    /**
     * 创建表
     * create 't_person','f1','f2','f3'
     *
     * @throws Exception
     */
    @Test
    public void createTableTest() throws Exception {
        // 创建表描述对象
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        // 创建列簇描述对象
        HColumnDescriptor column1 = new HColumnDescriptor(f1);
        // 设置保存数据的最大半本数量是3
        column1.setMaxVersions(3);

        HColumnDescriptor column2 = new HColumnDescriptor(f2);

        table.addFamily(column1);
        table.addFamily(column2);

        admin.createTable(table);

        admin.close();
        conn.close();
    }


    /**
     * 删除表
     * disable 't_person'
     * drop 't_person'
     *
     * @throws Exception
     */
    @Test
    public void dropTableTest() throws Exception {
        // 先停用表
        admin.disableTable(TableName.valueOf(tableName));

        // 再删除表
        admin.deleteTable(TableName.valueOf(tableName));

        admin.close();
        conn.close();
    }


    /**
     * 修改表添加列簇
     * alter 't_person','f4'
     *
     * @throws Exception
     */
    @Test
    public void modifyTableTest() throws Exception {
        // 取出旧的的表的描述信息
        HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor column = new HColumnDescriptor(f3);
        // 设置布隆过滤器
        column.setBloomFilterType(BloomType.ROWCOL);

        table.addFamily(column);

        admin.modifyTable(TableName.valueOf(tableName), table);

        admin.close();
        conn.close();
    }

    /**
     * 查看表定义信息
     * desc 't_person'
     *
     * @throws Exception
     */
    @Test
    public void descTableTest() throws Exception {
        HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor[] columnFamilies = table.getColumnFamilies();
        for (HColumnDescriptor hcd : columnFamilies) {
            logger.info("HColumn: {}", Bytes.toString(hcd.getName()));
        }
        admin.close();
        conn.close();
    }


}
