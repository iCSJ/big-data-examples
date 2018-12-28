package com.andy.hadoop.flow;

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
 * <p> 统计用户上网流量
 *
 * @author leone
 * @since 2018-12-27
 **/
public class FlowMain {

    static class FlowMapper extends Mapper<LongWritable, Text, Flow, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lies = value.toString();
            String[] fields = lies.split("");
            try {
                String phone = fields[1];
                
            } catch (Exception e) {

            }



        }
    }

    static class FlowReduce extends Reducer<Flow, Text, Text, Flow> {

        @Override
        protected void reduce(Flow key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance();

        job.setJarByClass(FlowMain.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        job.setMapOutputKeyClass(Flow.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path path = new Path(args[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path);
        }

        FileOutputFormat.setOutputPath(job, path);

        job.waitForCompletion(true);

    }


}
