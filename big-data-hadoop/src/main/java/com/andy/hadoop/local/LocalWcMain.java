package com.andy.hadoop.local;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p> 本地调试模式
 *
 * @author leone
 * @since 2018-12-27
 **/
public class LocalWcMain {

    private static String inputPath = "E:\\tmp\\hadoop\\input1\\words.txt";

    private static String outputPath = "E:\\tmp\\hadoop\\output";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(LocalWcMain.class);

        job.setMapperClass(WcLocalMapper.class);
        job.setReducerClass(WcLocalReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));

        Path path = new Path(outputPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
