package com.andy.hbase;

import com.andy.common.utils.RandomValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p> HBase crud
 *
 * @author leone
 * @since 2018-12-16
 **/
public class HBaseCrudTest {

    private static final Logger logger = LoggerFactory.getLogger(HBaseCrudTest.class);

    private String tableName = "t_person";

    private String f1 = "f1";

    private String f3 = "f3";

    private Connection conn;

    private Configuration conf;

    @Before
    public void init() throws Exception {
        // 创建连接对象,会自动加载HBase配置文件
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node-2:2181,node-3:2181,node-4:2181");
        conn = ConnectionFactory.createConnection(conf);
    }


    /**
     * DML 操作，向 HBase 插入数据
     * put 't_person','r2','f1:c1','v1'
     *
     * @throws Exception
     */
    @Test
    public void insertTest() throws Exception {
        // 获取指定表对象，进行dml操作
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> rows = new ArrayList<>();

        /*
         * 0 代表前面补充0
         * 4 代表长度为4
         * d 代表参数为正数型
         */
        for (int i = 1; i <= 20; i++) {
            Put row = new Put(Bytes.toBytes(String.format("%04d", i)));
            row.addColumn(Bytes.toBytes(f1), Bytes.toBytes("name"), Bytes.toBytes(RandomValue.getName()));
            row.addColumn(Bytes.toBytes(f1), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(RandomValue.getNumber(50))));
            String f2 = "f2";
            row.addColumn(Bytes.toBytes(f2), Bytes.toBytes("address"), Bytes.toBytes(RandomValue.getAddress()));
            rows.add(row);
        }
        logger.info("size:{}", rows.size());
        table.put(rows);
        conn.close();
    }

    /**
     * DML 操作，删除数据 HBase 中数据
     * delete 't_person','r1','f1:c1'
     *
     * @throws Exception
     */
    @Test
    public void deleteTest() throws Exception {
        // 获取指定表对象，进行dml操作
        Table table = conn.getTable(TableName.valueOf(tableName));

        Delete delete1 = new Delete(Bytes.toBytes(String.format("%04d", 1)));

        Delete delete2 = new Delete(Bytes.toBytes(String.format("%04d", 2)));
        delete2.addColumn(Bytes.toBytes(f1), Bytes.toBytes("name"));

        List<Delete> deleteList = new ArrayList<>();
        deleteList.add(delete1);
        deleteList.add(delete2);

        table.delete(deleteList);

        conn.close();
    }


    /**
     * DML 操作，删除数据 HBase 中数据
     * deleteall 't_person','r1'
     *
     * @throws Exception
     */
    @Test
    public void deleteAllTest() throws Exception {
        // 获取指定表对象，进行dml操作
        Table table = conn.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(String.format("%04d", 3)));

        table.delete(delete);

        conn.close();
    }

    /**
     * DML 操作，修改数据 HBase 中数据
     *
     * @throws Exception
     */
    @Test
    public void updateTest() throws Exception {
        // 获取指定表对象，进行dml操作
        Table table = conn.getTable(TableName.valueOf(tableName));

        conn.close();
    }


    /**
     * DML 操作 HBase 查询数据
     * get 't_person','r2'
     *
     * @throws Exception
     */
    @Test
    public void getTest() throws Exception {
        // 获取指定表对象，进行dml操作
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Get> gets = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            Get get = new Get(Bytes.toBytes(String.valueOf(String.format("%04d", i))));
            gets.add(get);
        }
        Result[] results = table.get(gets);
        for (Result result : results) {
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                // 行键的字节数组
                byte[] rowArray = cell.getRowArray();
                // 列簇的字节数组
                byte[] familyArray = cell.getFamilyArray();
                // 列名的字节数组
                byte[] qualifierArray = cell.getQualifierArray();
                // value的字节数组
                byte[] valueArray = cell.getValueArray();
                logger.info("行键:{} \t 列簇:{} \t key:{} \t value:{}", new Object[]{new String(rowArray, cell.getRowOffset(), cell.getRowLength()), new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()), new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()), new String(valueArray, cell.getValueOffset(), cell.getValueLength())});
            }
        }

        conn.close();
    }

    /**
     * DML 操作 HBase 查询数据
     * scan 't_person'
     *
     * @throws Exception
     */
    @Test
    public void scanTest() throws Exception {
        // 获取指定表对象，进行dml操作
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 可以指定开始行键和结束行键
        Scan scan = new Scan(Bytes.toBytes(String.format("%04d", 2)), Bytes.toBytes(String.format("%04d", 5)));

        ResultScanner result = table.getScanner(scan);

        for (Result rs : result) {
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
                logger.info("行键:{} \t 列簇:{} \t key:{} \t value:{}", new Object[]{new String(rowArray, cell.getRowOffset(), cell.getRowLength()), new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()), new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()), new String(valueArray, cell.getValueOffset(), cell.getValueLength())});
            }
        }
        conn.close();
    }


    /**
     * 查询某列数据的某个版本
     *
     * @throws Exception
     */
    @Test
    public void getVersionTest() throws Exception {
        HTable htable = new HTable(conf, tableName);

        Get get = new Get(Bytes.toBytes(String.format("%04d", 2)));

        get.addColumn(Bytes.toBytes(f1), Bytes.toBytes("name"));
        get.setMaxVersions(2);
        Result result = htable.get(get);
        for (KeyValue cell : result.list()) {
            byte[] rowArray = cell.getRowArray();
            // 列簇的字节数组
            byte[] familyArray = cell.getFamilyArray();
            // 列名的字节数组
            byte[] qualifierArray = cell.getQualifierArray();
            // value的字节数组
            byte[] valueArray = cell.getValueArray();
            logger.info("行键:{} \t 列簇:{} \t key:{} \t value:{} \ttimestamp:{}", new Object[]{new String(rowArray, cell.getRowOffset(), cell.getRowLength()), new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()), new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()), new String(valueArray, cell.getValueOffset(), cell.getValueLength()), cell.getTimestamp()});
        }
        conn.close();
    }

}
