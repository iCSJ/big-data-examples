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

            Long productId;
            String[] fields = value.toString().split(",");

            // 通过文件名称判断是那种文件
            if (name.startsWith("order")) {
                // orderId   productId   createTime   totalAmount
                productId = Long.parseLong(fields[1]);
                joinBean.set(Long.parseLong(fields[0]), productId, fields[2], Long.parseLong(fields[3]), "", 0D, "0");
            } else {
                // productId    productName    productPrice
                productId = Long.parseLong(fields[0]);
                joinBean.set(0L, productId, "", 0L, fields[1], Double.parseDouble(fields[2]), "1");
            }
            k.set(String.valueOf(productId));
            context.write(k, joinBean);
        }
    }

    /**
     * reduce业务逻辑
     */
    static class JoinReduce extends Reducer<Text, JoinBean, JoinBean, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
            // reduce 端的productId 是相同的也就是同一个 product 实体
            JoinBean product = new JoinBean();
            ArrayList<JoinBean> orderBeans = new ArrayList<>();
            // 0: 商品, 1: 订单
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
                bean.setProductName(product.getProductName());
                bean.setProductPrice(product.getProductPrice());
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
