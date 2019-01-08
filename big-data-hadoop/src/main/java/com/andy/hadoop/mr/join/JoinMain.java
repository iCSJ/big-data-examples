package com.andy.hadoop.mr.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-30
 **/
public class JoinMain {


    /**
     * mapper 业务逻辑
     */
    static class JoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {

        JoinBean joinBean = new JoinBean();

        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();

            // 得到文件名称
            String name = inputSplit.getPath().getName();

            String productId;

            // 通过文件名称判断是那种文件
            if (name.startsWith("order")) {
                String fields[] = value.toString().split(",");
                // id   date    productId   amount
                productId = fields[2];
                joinBean.set(Integer.parseInt(fields[0]), fields[1], productId, Integer.parseInt(fields[3]), "", 0, 0F, "0");
            } else {
                String fields[] = value.toString().split(",");
                // productId    name    categoryId  price
                productId = fields[0];
                joinBean.set(0, "", productId, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");
            }

            k.set(productId);
            context.write(k, joinBean);
        }
    }

    /**
     * reduce业务逻辑
     */
    static class JoinReduce extends Reducer<Text, JoinBean, JoinBean, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
            JoinBean product = new JoinBean();
            ArrayList<JoinBean> orderBeans = new ArrayList<>();
            for (JoinBean bean : values) {
                if ("1".equals(bean.getFlag())) {
                    try {
                        BeanUtils.copyProperties(product, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    JoinBean orderBean = new JoinBean();
                    try {
                        BeanUtils.copyProperties(orderBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    orderBeans.add(orderBean);
                }
            }

            for (JoinBean bean : orderBeans) {
                bean.setName(product.getName());
                bean.setCategoryId(product.getCategoryId());
                bean.setPrice(product.getPrice());

                // 写出
                context.write(bean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        if (args.length < 1) {
            System.err.println("please set process arguments!");
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinMain.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path path = new Path(args[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);


        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);

    }


}
