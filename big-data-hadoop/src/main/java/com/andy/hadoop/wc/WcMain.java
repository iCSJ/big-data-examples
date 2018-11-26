package com.andy.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>
 *
 * @author Leone
 * @since 2018-11-21
 **/
public class WcMain {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();


        Job job = Job.getInstance();
        // 设置运行/处理该作业的类
        job.setJarByClass(WcMain.class);
        job.setJobName("WordCount");

        //设置这个作业输入数据的路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //设置这个作业输出结果的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置实现了Map步的类
        job.setMapperClass(WordCountMapper.class);

        //设置实现了Reduce步的类
        job.setReducerClass(WordCountReducer.class);


        //设置输出结果key的类型
        job.setOutputKeyClass(Text.class);

        //设置输出结果value的类型
        job.setOutputValueClass(IntWritable.class);

        //执行作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
