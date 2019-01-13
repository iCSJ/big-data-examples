package com.andy.hadoop.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * <p>
 *
 * @author leone
 * @since 2018-05-06
 **/
public class HdfsClientTest {

    FileSystem fs = null;

    /**
     * 初始化
     */
    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://node-1:9000/"), conf, "root");
    }


    /**
     * 上传文件
     *
     * @throws IOException
     */
    @Test
    public void putTest() throws IOException {
        fs.copyFromLocalFile(new Path("file:///D:/tmp/hadoop/input/hello.txt"), new Path("hdfs://node-1:9000/data/hello.txt"));
    }


    /**
     * 下载文件
     *
     * @throws Exception
     * @throws IOException
     */
    @Test
    public void download() throws Exception, IOException {
        fs.copyToLocalFile(new Path("/data/input/hello.txt"), new Path("file:///E:/hadoop/hello.txt"));
    }


    /**
     * 创建目录
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void mkdirTest() throws IllegalArgumentException, IOException {
        fs.mkdirs(new Path("/data/input/"));
    }

    /**
     * 删除文件或目录
     */
    @Test
    public void deleteTest() throws IOException {
        fs.delete(new Path("/data"), true);
    }


    /**
     * 移动文件
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void mvFileTest() throws IllegalArgumentException, IOException {
        fs.rename(new Path("/data/input/hello.txt"), new Path("/data/output/hello.txt"));
    }


}
