package com.xiaozhameng.demo02.inputformat.cust;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;

/**
 * 功能描述：驱动类
 *
 * @author: xiaozhameng
 * @date: 2020/7/23 1:31 下午
 */
public class SequenceFileDriver {
    public static void main(String[] args) throws Exception {
        String in = "/Users/xiaozhameng/Desktop/TESTFILE/input/demo1";
        String out = "/Users/xiaozhameng/Desktop/TESTFILE/output";
        FileUtils.deleteDirectory(new File(out));

        // 1、封装任务

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置jar路径
        job.setJarByClass(SequenceFileDriver.class);

        // 3、设置map和reduce 类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 4、设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 5、设置最终的输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置job的一些额外配置
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 6、输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        // 7、提交任务
        boolean result = job.waitForCompletion(true);

        // 8、退出
        System.exit(result ? 0 : 1);
    }
}
