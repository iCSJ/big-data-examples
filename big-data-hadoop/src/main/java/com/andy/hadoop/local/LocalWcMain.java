package com.andy.hadoop.local;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p> 本地调试模式
 *
 * @author leone
 * @since 2018-12-27
 **/
public class LocalWcMain {

    private static String inputPath = "E:\\tmp\\hadoop\\input";

    private static String outputPath = "E:\\tmp\\hadoop\\output";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(LocalWcMain.class);


        // 设置 job 的inputFormat 如果不设置默认是 TextInputFormat
//        job.setInputFormatClass(CombineFileInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

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
