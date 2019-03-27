package com.andy.hadoop.mr.call;

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

/**
 * 1550425978144,18012350867,2018-05-29 01:52:58,802
 * hdfs://node-1:9000/flume-1.7.0/logs/20190220
 * file:///e:/tmp/hadoop/output1
 * hdfs://node-1:9000/hadoop-2.7.7/output1
 *
 * @author leone
 * @since 2019-02-21
 **/
public class CallMain {

    static class CallMapper extends Mapper<LongWritable, Text, Text, CallBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split(",");
            CallBean call = new CallBean(lines[0], lines[1], lines[2], Integer.parseInt(lines[3]));
            context.write(new Text(lines[0]), call);
        }
    }

    static class CallReduce extends Reducer<Text, CallBean, Text, CallBean> {
        @Override
        protected void reduce(Text key, Iterable<CallBean> values, Context context) throws IOException, InterruptedException {
            final int[] sum = {0};
            values.forEach(e -> {
                sum[0] += e.getDuration();
            });
            context.write(key, new CallBean(key.toString(), sum[0]));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.println("please input arguments inputPath and outputPath!");
            System.exit(0);
        }
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node-1:9000");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname", "node-1");

        Job job = Job.getInstance();

        job.setJobName("call-log");
        job.setJar("/root/call-log.jar");

        job.setJarByClass(CallMain.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CallBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CallBean.class);

        job.setMapperClass(CallMapper.class);
        job.setReducerClass(CallReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }


}
