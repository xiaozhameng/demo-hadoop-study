package com.xiaozhameng.demo02.inputformat.combine;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * MapReduce 驱动类
 *
 * programArg: C:\Users\WJ\Desktop\temp\reduce\input C:\Users\WJ\Desktop\temp\reduce\output
 * @author xiaozhameng
 */
public class WordCountSplitDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        String in = "/Users/xiaozhameng/Desktop/TESTFILE/input/demo1/";
        String out = "/Users/xiaozhameng/Desktop/TESTFILE/output/demo1";
        // 计算前删除数据
        FileUtils.deleteDirectory(new File("/Users/xiaozhameng/Desktop/TESTFILE/output/"));

        // 1、封装任务
        String jobName = "My-WordCountJob";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);

        // 2、设置jar路径
        job.setJarByClass(WordCountSplitDriver.class);

        // 3、设置map和reduce 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终的输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        // 参数其他值设置
        // 设置InputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置 4m
        CombineTextInputFormat.setMaxInputSplitSize(job,1024 * 1024 * 4);

        // 7、提交任务
        boolean result = job.waitForCompletion(true);

        // 8、退出
        System.exit(result ? 0 : 1);
    }

}
