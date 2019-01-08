package com.andy.hadoop.mr.join.side;

import com.andy.hadoop.mr.join.JoinBean;
import com.andy.hadoop.mr.join.JoinMain;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-06
 **/
public class SideJoinMain {

    /**
     * mapper 业务逻辑
     */
    static class SideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> productMap = new HashMap<>();

        /**
         * 加载文件的数据到task的工作目录
         *
         * @param context
         * @throws IOException
         */
        @Override
        protected void setup(Context context) throws IOException {
            // 初始化操作
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("E:/tmp/hadoop/input1/product.log")));
            String line;
            while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
                String[] fields = line.split(",");
                productMap.put(fields[0], fields[1]);
            }
            bufferedReader.close();
        }

        JoinBean joinBean = new JoinBean();

        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String orderLines = value.toString();
            String[] fields = orderLines.split(",");

            String productName = productMap.get(fields[2]);
            k.set(orderLines + "," + productName);
            context.write(k, NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration();
        if (args.length < 1) {
            System.err.println("please set process arguments!");
        }

        Job job = Job.getInstance(conf);

        // 将商品表缓存到task的工作节点的目录中
        job.addCacheFile(new URI("file:/E:/tmp/hadoop/input1/product.log"));
        job.setJarByClass(JoinMain.class);

        job.setMapperClass(SideJoinMapper.class);
        // 设置指定缓存文件
//        job.addArchiveToClassPath();
//        job.addCacheArchive();


        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reduce task的数量
        job.setNumReduceTasks(0);

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
