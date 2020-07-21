package com.xiaozhameng.demo02.inputformat.keyvalue;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String in = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\input\\demo2";
        String out = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\output";
        FileUtils.deleteDirectory(new File(out));

        // 1、获取job
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);

        // 2、设置jar 路径
        job.setJarByClass(KVTextDriver.class);

        // 3、设置关联Mapper 和 Reducer类
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        // 4、设置Mapper输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终输出的key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(in));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(out));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
