package com.andy.hadoop.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-30
 **/
public class JoinMain {


    static class JoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {

        JoinBean joinBean = new JoinBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            int productId = 0;
            if (name.startsWith("order")) {
                String fields[] = value.toString().split(",");
                productId = Integer.parseInt(fields[3]);
                joinBean.set(Integer.parseInt(fields[0]), fields[1], fields[2], productId, "", 0, 0F, "0");
            } else {
                String fields[] = value.toString().split(",");
                productId = Integer.parseInt(fields[3]);
                joinBean.set(Integer.parseInt(fields[0]), fields[1], fields[2], productId, "", 0, 0F, "0");
            }

//            context.write();

        }
    }

    static class JoinReduce extends Mapper<Text, JoinBean, Text, Integer> {


    }

    public static void main(String[] args) {


    }


}
