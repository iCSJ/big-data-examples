package com.andy.hbase.parquet;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class ParquetComplexTest {

    private static final Logger logger = LoggerFactory.getLogger(ParquetComplexTest.class);

    private static Random r = new Random();

    private static String inputPath = "e:\\tmp\\input\\parquet\\user.parquet";

    private static String outputPath = "e:\\tmp\\input\\parquet\\user.parquet";

    private static MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("user_id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("username")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
            .repeatedGroup()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("order_id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("create_time")
            .named("Order")
            .named("User");

    /**
     * schema 约束信息
     *
     * message User {
     *   required int64 user_id;
     *   required binary username (UTF8);
     *   required int32 age;
     *   repeated group Order {
     *     required int32 order_id;
     *     required binary create_time (UTF8);
     *   }
     * }
     */

    public static void main(String[] args) throws Exception {
        // System.out.println(schema.toString());
         readParquet();
        // writeParquet();
    }

    /**
     * 写出parquet文件
     *
     * @throws Exception
     */
    public static void writeParquet() throws Exception {
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(new Path(outputPath))
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                //.withConf(configuration)
                .withType(schema);
        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        for (int i = 0; i < 10000; i++) {
            Group group = groupFactory.newGroup()
                    .append("user_id", (long) i)
                    .append("username", UUID.randomUUID().toString())
                    .append("age", i);
            group.addGroup("Order").append("order_id", i)
                    .append("create_time", "2019-03-21 10:23:0" + r.nextInt(9));
            writer.write(group);
        }
        writer.close();
    }

    /**
     * 读取复杂parquet文件
     *
     * @throws Exception
     */
    public static void readParquet() throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<>(new Path(inputPath), readSupport);
        Group line;
        while ((line = reader.read()) != null) {
            // System.out.println(line.toString());
            long user_id = line.getLong("user_id", 0);
            String username = line.getString("username", 0);

            Group order = line.getGroup("Order", 0);
            int order_id = order.getInteger("order_id", 0);
            String create_time = order.getString(1, 0);
            logger.info("user_id: {}, username: {}, Order[order_id: {}, create_time: {}]", new Object[]{user_id, username, order_id, create_time});
        }
        reader.close();
    }

}
