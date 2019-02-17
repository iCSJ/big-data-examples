package com.andy.hadoop.mr.flow;

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
 * <p> 统计用户上网流量
 *
 * @author leone
 * @since 2018-12-27
 **/
public class FlowMain {

    static class FlowMapper extends Mapper<LongWritable, Text, Text, Flow> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lies = value.toString();
            String[] fields = lies.split("\t");
            System.out.println(Arrays.toString(fields));
            try {
                String phone = fields[0];
                long upFlow = Long.parseLong(fields[fields.length - 3]);
                long downFlow = Long.parseLong(fields[fields.length - 2]);
                Flow flow = new Flow(upFlow, downFlow);
                context.write(new Text(phone), flow);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class FlowReduce extends Reducer<Text, Flow, Text, Flow> {

        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
            final long[] sumUp = {0};
            final long[] sumDown = {0};
            values.forEach(e -> {
                sumUp[0] += e.getUpFlow();
                sumDown[0] += e.getDownFlow();
            });
            Flow flow = new Flow(sumUp[0], sumDown[0]);
            context.write(key, flow);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 本地运行配置，默认为本地运行配置
//        conf.set("mapreduce.app-submission.cross-platform", "true");
//        conf.set("mapred.job.tracker", "local");
//        conf.set("fs.default.name", "file:///");

        // 提交到集群配置 设置job运行的文件系统和资源调度系统
//        conf.set("fs.defaultFS", "hdfs://node-1:9000");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "node-1");

        Job job = Job.getInstance();

        job.setJarByClass(FlowMain.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

    }


}
