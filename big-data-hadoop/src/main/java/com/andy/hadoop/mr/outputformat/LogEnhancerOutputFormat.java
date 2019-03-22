package com.andy.hadoop.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * <p>
 *
 * @author leone
 * @since 2019-03-22
 **/
public class LogEnhancerOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path enhancePath = new Path("hdfs://node-1:9000/flow/enhanced.log");
        Path toCrawlPath = new Path("hdfs://node-1:9000/flow/tocrawl.log");

        FSDataOutputStream enhanceOut = fs.create(enhancePath);
        FSDataOutputStream toCrawlOut = fs.create(toCrawlPath);

        return new CustomerRecordWriter(enhanceOut, toCrawlOut);
    }

    static class CustomerRecordWriter extends RecordWriter<Text, NullWritable> {

        FSDataOutputStream enhanceOut;
        FSDataOutputStream toCrawlOut;

        public CustomerRecordWriter(FSDataOutputStream enhanceOut, FSDataOutputStream toCrawlOut) {
            this.enhanceOut = enhanceOut;
            this.toCrawlOut = toCrawlOut;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            // 有了数据，你来负责写到目的地 hdfs
            // 判断，进来内容如果是带 tocrawl 的，就往待爬清单输出流中写 toCrawlOut
            if (key.toString().contains("tocrawl")) {
                toCrawlOut.write(key.toString().getBytes());
            } else {
                enhanceOut.write(key.toString().getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (toCrawlOut != null) {
                toCrawlOut.close();
            }
            if (enhanceOut != null) {
                enhanceOut.close();
            }
        }

    }
}
