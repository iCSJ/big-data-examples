package com.andy.hbase.parquet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Random;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class ParquetTest {

    public static void main(String[] args) throws IOException {
        parquetWriter("e:\\tmp\\input\\parquet\\a.parquet");
    }

    public static void parquetReaderV2(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        Builder<Group> reader = ParquetReader.builder(readSupport, new Path(inPath));
        ParquetReader<Group> build = reader.build();
        Group line = null;
        while ((line = build.read()) != null) {
            Group time = line.getGroup("time", 0);
            //通过下标和字段名称都可以获取
            /*System.out.println(line.getString(0, 0)+"\t"+
            line.getString(1, 0)+"\t"+
            time.getInteger(0, 0)+"\t"+
            time.getString(1, 0)+"\t");*/
            System.out.println(line.getString("city", 0) + "\t" +
                    line.getString("ip", 0) + "\t" +
                    time.getInteger("ttl", 0) + "\t" +
                    time.getString("ttl2", 0) + "\t");

            //System.out.println(line.toString());

        }
        System.out.println("读取结束");
    }

    //新版本中new ParquetReader()所有构造方法好像都弃用了,用上面的builder去构造对象
    static void parquetReader(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<>(new Path(inPath), readSupport);
        Group line;
        while ((line = reader.read()) != null) {
            System.out.println(line.toString());
        }
        System.out.println("读取结束");

    }

    /**
     * @param outPath 输出Parquet格式
     * @throws IOException
     */
    public static void parquetWriter(String outPath) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message User {" +
                " required int64 userId;" +
                " required binary username (UTF8);" +
                " repeated group Order {" +
                " required int32 orderId;" +
                " required binary createTime (UTF8);" +
                "}" +
                "}");
        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(outPath);
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, configuration);
        ParquetWriter<Group> writer = new ParquetWriter<>(path, configuration, writeSupport);
        Random r = new Random();
        for (int i = 0; i < 10000; i++) {
            Group group = factory.newGroup().append("userId", i).append("username", "name" + i);
            Group groupInfo = group.addGroup("Order");
            groupInfo.append("orderId", r.nextInt(99) + 1);
            groupInfo.append("createTime", "2019-03-21 10:23:0" + r.nextInt(9));
            writer.write(group);
        }
        System.out.println("write end");
        writer.close();
    }

}
