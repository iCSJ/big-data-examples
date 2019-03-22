package com.andy.hadoop.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomReader extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit;

    private Configuration conf;

    private BytesWritable value = new BytesWritable();

    private boolean processed = false;

    public CustomReader() {
        super();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // 获取传递过来的数据
        this.fileSplit = (FileSplit) inputSplit;
        conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            // 获取文件系统
            FileSystem fileSystem = FileSystem.get(conf);
            // 获取输入流
            Path path = fileSplit.getPath();
            FSDataInputStream fis = fileSystem.open(path);
            // 读取数据，定义缓冲区，缓冲区长度大小设置为文件的大小
            byte[] buf = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(fis, buf, 0, buf.length);
            // 写入到v
            value.set(buf, 0, buf.length);
            // 是否继续读文件，防止重复读
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

}
