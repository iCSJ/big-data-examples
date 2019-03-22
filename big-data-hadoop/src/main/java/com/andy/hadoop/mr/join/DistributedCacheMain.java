package com.andy.hadoop.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p> 在map端join
 *
 * @author leone
 * @since 2019-01-06
 **/
public class DistributedCacheMain {

    private static Configuration conf = new Configuration();

    /**
     * mapper 业务逻辑
     */
    static class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> productMap = new HashMap<>();

        FileReader in = null;

        BufferedReader reader = null;

        String localPath = null;

        String uirPath = null;

        /**
         * 加载文件的数据到task的工作目录
         *
         * @param context
         * @throws IOException
         */
        @Override
        protected void setup(Context context) throws IOException {
            // 获取 cache file 的本地绝对路径，测试验证用
            Path[] files = context.getLocalCacheFiles();
            for (Path path : files) {
                System.err.println(path.toString());
            }
            // 为了解决windows文件路径问题
            localPath = files[0].toString().split(":")[1];

            // localPath = files[0].toString();
            URI[] cacheFiles = context.getCacheFiles();

            //这里读的数据是map task所在机器本地工作目录中的一个小文件
            reader = new BufferedReader(new FileReader("d:" + localPath));
            String line;
            while (null != (line = reader.readLine())) {
                String[] fields = line.split(",");
                productMap.put(fields[0], fields[1] + "," + fields[2]);
            }
            IOUtils.closeStream(reader);
            IOUtils.closeStream(in);
        }

        JoinBean joinBean = new JoinBean();

        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            // 得到文件名称
            String name = inputSplit.getPath().getName();

            if (name.startsWith("order")) {
                String orderLines = value.toString();
                String[] fields = orderLines.split(",");
                String productName = productMap.get(fields[1]);
                k.set(orderLines + "," + productName);
                context.write(k, NullWritable.get());
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if (args.length < 1) {
            System.err.println("please set process arguments!");
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(JoinMain.class);

        // 不需要reducer
        job.setNumReduceTasks(0);
        job.setMapperClass(DistributedCacheMapper.class);

        /**
         * 不同文件类型的添加方法：
         *
         * // 缓存jar包到task运行节点的classpath中
         * job.addArchiveToClassPath(archive);
         *
         * // 缓存普通文件到task运行节点的classpath中
         * job.addFileToClassPath(file);
         *
         * // 缓存压缩包文件到task运行节点的工作目录
         * job.addCacheArchive(uri);
         *
         * // 缓存普通文件到task运行节点的工作目录
         * job.addCacheFile(uri)
         */

        // 将商品表缓存到task的工作节点的目录中
        job.addCacheFile(new URI("file:/E:/tmp/hadoop/input5/product.txt"));
        job.addCacheFile(new URI("file:/E:/tmp/hadoop/input5/order-1.txt"));

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
