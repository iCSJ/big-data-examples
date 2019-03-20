package com.andy.hadoop.mr.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-07
 **/
public class FriendMainTwo {


    /**
     * mapper 业务逻辑
     */
    static class FriendMapperTwo extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 得到上一步输处的结果
            String[] person_friend = value.toString().split("\t");
            String friend = person_friend[0];
            String[] person = person_friend[1].split(",");
            Arrays.sort(person);
            for (int i = 0; i < person.length - 2; i++) {
                for (int j = 0; j < person.length - 1; j++) {
                    context.write(new Text(person[i] + "-" + person[j]), new Text(friend));
                }
            }
        }
    }

    /**
     * reducer业务逻辑
     */
    static class FriendReducerTwo extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text friend : values) {
                sb.append(friend).append(" ");
            }
            context.write(key, new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FriendMainTwo.class);

        job.setMapperClass(FriendMapperTwo.class);
        job.setReducerClass(FriendReducerTwo.class);


        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        // 如果目标文件夹存在就删除
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path output = new Path(args[1]);
        FileSystem fileSystem = output.getFileSystem(conf);
        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


}
