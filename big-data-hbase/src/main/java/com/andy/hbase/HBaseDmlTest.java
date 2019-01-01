package com.andy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <p> HBase crud
 *
 * @author leone
 * @since 2018-12-16
 **/
public class HBaseDmlTest {

    private static final Logger logger = LoggerFactory.getLogger(HBaseDmlTest.class);

    private Connection conn;

    @Before
    public void init() throws Exception {
        // 创建连接对象,会自动加载HBase配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node-2:2181,node-3:2181,node-4:2181");
        conn = ConnectionFactory.createConnection(conf);
    }


    /**
     * DML 操作 HBase 插入数据
     *
     * @throws Exception
     */
    @Test
    public void insertTest() throws Exception {

        // 获取指定表对象，进行dml操作
        Table user_info = conn.getTable(TableName.valueOf("user_info"));

        Put put1 = new Put(Bytes.toBytes(002));
        put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三"));
        put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(23));
        put1.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("北京"));

        Put put2 = new Put(Bytes.toBytes(005));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("李四"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(25));
        put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("上海"));

        List<Put> puts = Arrays.asList(put1, put2);
        System.out.println(puts.size());
        user_info.put(puts);

        conn.close();
    }

    /**
     * DML 操作 HBase 删除数据
     *
     * @throws Exception
     */
    @Test
    public void deleteTest() throws Exception {

        // 获取指定表对象，进行dml操作
        Table user_info = conn.getTable(TableName.valueOf("user_info"));

        Delete delete1 = new Delete(Bytes.toBytes("001"));

        Delete delete2 = new Delete(Bytes.toBytes("002"));
        delete2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"));

        List<Delete> deleteList = new ArrayList<>();
        deleteList.add(delete1);
        deleteList.add(delete2);

        user_info.delete(deleteList);

        conn.close();
    }

    /**
     * DML 操作 HBase 修改数据
     *
     * @throws Exception
     */
    @Test
    public void updateTest() throws Exception {

        // 获取指定表对象，进行dml操作
        Table user_info = conn.getTable(TableName.valueOf("user_info"));


        conn.close();
    }


    /**
     * DML 操作 HBase 查询数据
     *
     * @throws Exception
     */
    @Test
    public void selectTest() throws Exception {
        // 获取指定表对象，进行dml操作
        Table user_info = conn.getTable(TableName.valueOf("user_info"));
        Get get = new Get(Bytes.toBytes("1"));
        Result result = user_info.get(get);
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            byte[] rowArray = cell.getRowArray();
            // 列簇的字节数组
            byte[] familyArray = cell.getFamilyArray();
            // 列名的字节数组
            byte[] qualifierArray = cell.getQualifierArray();
            // value的字节数组
            byte[] valueArray = cell.getValueArray();
            logger.info("行键:" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()) + "\t列簇名:" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()) + "\t列名:" + new String(qualifierArray, cell.getFamilyOffset(), cell.getFamilyLength()) + "\tvalue:" + new String(valueArray, cell.getQualifierOffset(), cell.getQualifierLength()));
        }
        conn.close();
    }

    /**
     * DML 操作 HBase 查询数据
     *
     * @throws Exception
     */
    @Test
    public void scanTest() throws Exception {

        // 获取指定表对象，进行dml操作
        Table user_info = conn.getTable(TableName.valueOf("user_info"));
        Scan scan = new Scan(Bytes.toBytes("4"), "1".getBytes());

        ResultScanner result = user_info.getScanner(scan);
        Iterator<Result> iterator = result.iterator();
        while (iterator.hasNext()) {
            Result rs = iterator.next();
            CellScanner cellScanner = rs.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                byte[] rowArray = cell.getRowArray();
                // 列簇的字节数组
                byte[] familyArray = cell.getFamilyArray();
                // 列名的字节数组
                byte[] qualifierArray = cell.getQualifierArray();
                // value的字节数组
                byte[] valueArray = cell.getValueArray();
                logger.info("行键:" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()) + "\t列簇名:" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()) + "\t列名:" + new String(qualifierArray, cell.getFamilyOffset(), cell.getFamilyLength()) + "\tvalue:" + new String(valueArray, cell.getQualifierOffset(), cell.getQualifierLength()));
            }


        }
        conn.close();
    }


    /**
     * 批量插入测试
     */
    @Test
    public void batchPutTest() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        ArrayList<Put> puts = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Put put = new Put(Bytes.toBytes(i + ""));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三" + 1));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("1" + 1));
            put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("上海" + 1));
            puts.add(put);
        }
        table.put(puts);
    }


}
