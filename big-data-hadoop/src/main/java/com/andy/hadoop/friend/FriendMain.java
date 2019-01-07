package com.andy.hadoop.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-07
 **/
public class FriendMain {

    static class FriendMapperOne extends Mapper<LongWritable, Text, Text, LongWritable> {


    }


    static class FriendReducerOnw extends Reducer<Text, LongWritable, Text, LongWritable> {


    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);





        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);

    }


}
