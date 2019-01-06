package com.andy.hadoop.index;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-06
 **/
public class InvertedIndexMainTwo {

    static class InvertedIndexMapperTwo extends Mapper<LongWritable, Text, Text, Text> {
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words_file = line.split("--");
            context.write(new Text(words_file[0]), new Text(words_file[1]));
        }

    }


    /**
     * reduce业务逻辑
     */
    static class InvertedIndexReducerTwo extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString()).append("\t");
            }
            System.out.println(sb.toString());
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(InvertedIndexMainTwo.class);

        job.setMapperClass(InvertedIndexMapperTwo.class);
        job.setReducerClass(InvertedIndexReducerTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path path = new Path(args[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

}
