package com.andy.hadoop.mr.local;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>
 *
 * @author leone
 * @since 2018-11-21
 **/
public class LocalWcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lines = line.split(" ");
        for (String words : lines) {
            context.write(new Text(words), new IntWritable(1));
        }
    }
}
