package com.xiaozhameng.demo02.inputformat.nline;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * NL 测试
 */
public class NLineDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        String in = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\input\\nline";
        String out = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\output";
        FileUtils.deleteDirectory(new File(out));

        // 创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置使用NLineInputFormat 处理记录
        NLineInputFormat.setNumLinesPerSplit(job,3);
        job.setInputFormatClass(NLineInputFormat.class);

        // 指定jar包
        job.setJarByClass(NLineDriver.class);

        // 关联mapper & reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        // 设置mapper 的key，value 输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置最终的输出的key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(in));
        FileOutputFormat.setOutputPath(job,new Path(out));

        // job 提交
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
