package com.andy.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


    }

}
