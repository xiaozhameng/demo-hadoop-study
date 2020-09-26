package com.xiaozhameng.demo02.shuffer.combiner;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
public class WordCountDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        // 样例文件在resource 文件夹下面 phone_data.txt
        String in = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\input\\demo2";
        String out = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\output";
        FileUtils.deleteDirectory(new File(out));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置jar路径
        job.setJarByClass(WordCountDriver.class);

        // 3、设置map和reduce 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终的输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置job的一些额外配置
        // job.setInputFormatClass(CombineTextInputFormat.class);
        // CombineTextInputFormat.setMaxInputSplitSize(job,20*1024*1024);
        job.setCombinerClass(WordCountReducer.class);

        // 6、输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        // 7、提交任务
        boolean result = job.waitForCompletion(true);

        // 8、退出
        System.exit(result ? 0 : 1);
    }
}
