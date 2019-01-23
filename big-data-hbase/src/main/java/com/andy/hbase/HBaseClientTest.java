package com.andy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * <p> HBase crud
 *
 * @author leone
 * @since 2018-12-16
 **/
public class HBaseClientTest {


    private Connection conn;

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
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 创建列簇描述对象
        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(f1);
        // 设置保存数据的最大半本数量是3
        hColumnDescriptor1.setMaxVersions(3);

        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(f2);

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
        admin.disableTable(TableName.valueOf(tableName));

        // 再删除表
        admin.deleteTable(TableName.valueOf(tableName));

        admin.close();
        conn.close();
    }


    /**
     * 修改表
     *
     * @throws Exception
     */
    @Test
    public void alterTableTest() throws Exception {
        // 创建ddl描述对象
        Admin admin = conn.getAdmin();

        // 取出旧的的表的描述信息
        HTableDescriptor user_info = admin.getTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(f3);
        // 设置布隆过滤器
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);

        user_info.addFamily(hColumnDescriptor);

        admin.modifyTable(TableName.valueOf(f3), user_info);

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
        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(f1);
        // 设置布隆过滤器
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);

        hTableDescriptor.addFamily(hColumnDescriptor);

        admin.modifyTable(TableName.valueOf(tableName), hTableDescriptor);

        admin.close();
        conn.close();
    }


    /**
     * @throws Exception
     */
    @Test
    public void getRow() throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes("r1"));
        Result result = table.get(get);
        System.out.println(Arrays.toString(result.getRow()));
        table.close();
        conn.close();
    }




}
