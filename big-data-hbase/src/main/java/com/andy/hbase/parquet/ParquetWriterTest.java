package com.andy.hbase.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class ParquetWriterTest {

    private static Logger logger = LoggerFactory.getLogger(ParquetWriterTest.class);

    /*
     * 写 Parquet 格式数据需要 schema，读取的话自动识别 schema
     * 每一个字段有三个属性：重复数、数据类型和字段名，重复数可以是以下三种：
     *         required(出现1次)
     *         repeated(出现0次或多次)
     *         optional(出现0次或1次)
     * 每一个字段的数据类型可以分成两种：
     *         group(复杂类型)
     *         primitive(基本类型)
     * 数据类型有
     * INT64, INT32, BOOLEAN, BINARY, FLOAT, DOUBLE, INT96, FIXED_LEN_BYTE_ARRAY
     */

    private static String schemaStr = "message schema {"
            + "optional int64 userId;"
            + "optional binary account;"
            + "optional binary password;"
            + "optional int32 age;"
            + "optional int32 sex;"
            + "optional binary description;"
            + "optional boolean deleted;"
            + "optional binary createTime;}";

    private static MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

    //import org.apache.parquet.schema.Types;
    /*MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("city")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ip")
            .repeatedGroup().required(PrimitiveType.PrimitiveTypeName.INT32).named("ttl1")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ttl2")
            .named("time")
            .named("Pair");*/


    /**
     * 获取parquet的约束信息
     *
     * @throws Exception
     */
    public static void getSchema() throws Exception {
        Configuration configuration = new Configuration();
        ParquetMetadata readFooter;
        Path parquetFilePath = new Path("file:///E:/tmp/input/parquet/test.parquet");
        readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        logger.info(schema.toString());
    }

    // 生成parquet文件
    private static void parquetWriter() throws IOException, ParseException {
        Path file = new Path("file:///e:\\tmp\\input\\parquet\\test.parquet");
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(file)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                //.withConf(configuration)
                .withType(schema);

        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String[] userLog = {"1001", "james", "6265548", "18", "1", "good man", "false", "2019-02-06 00:00:00"};

        for (int i = 0; i < 1000; i++) {
            writer.write(groupFactory.newGroup()
                    .append("userId", Long.parseLong(userLog[0]))
                    .append("account", userLog[1] + i)
                    .append("password", userLog[2])
                    .append("age", Integer.parseInt(userLog[3]))
                    .append("sex", Integer.parseInt(userLog[4]))
                    .append("description", userLog[5])
                    .append("deleted", Boolean.parseBoolean(userLog[6]))
                    .append("createTime", userLog[7]));
        }
        writer.close();
    }


    /**
     * 读取parquet文件
     *
     * @throws IOException
     */
    private static void parquetReader() throws IOException {
        Path file = new Path("file:///e:\\tmp\\input\\parquet\\test.parquet");
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file);
        ParquetReader<Group> reader = builder.build();
        SimpleGroup group = (SimpleGroup) reader.read();
        logger.info("schema:" + group.getType().toString());
        logger.info("account:" + group.getString("account", 0));
    }


    public static void main(String[] args) throws Exception {
        getSchema();
    }

}
