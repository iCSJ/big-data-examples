package com.andy.hadoop.mr.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p> 日志清洗
 *
 * @author leone
 * @since 2019-03-21
 **/
public class WebLogCleanMain {

    static class WebLogCleaningMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();
        NullWritable v = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WebLogBean bean = WebLogParser.parser(line);
            if (!bean.isValid()) {
                return;
            }
            k.set(bean.toString());
            context.write(k, v);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WebLogCleanMain.class);
        job.setMapperClass(WebLogCleaningMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

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
