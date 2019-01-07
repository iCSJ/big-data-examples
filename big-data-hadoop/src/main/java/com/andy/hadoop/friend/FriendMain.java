package com.andy.hadoop.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-07
 **/
public class FriendMain {


    /**
     * mapper 业务逻辑
     */
    static class FriendMapperOne extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            String[] words = lines.split(",");


        }
    }


    /**
     * reducer业务逻辑
     */
    static class FriendReducerOnw extends Reducer<Text, LongWritable, Text, LongWritable> {


        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {


        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);



        job.setJarByClass(FriendMain.class);


        // 如果目标文件夹存在就删除
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path output = new Path(args[1]);
        FileSystem fileSystem = output.getFileSystem(conf);
        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);
        }


        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }


}
