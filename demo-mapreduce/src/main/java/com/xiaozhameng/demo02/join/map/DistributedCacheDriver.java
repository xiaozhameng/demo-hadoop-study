package com.xiaozhameng.demo02.join.map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.net.URI;

public class DistributedCacheDriver {

    public static void main(String[] args) throws Exception {

        String in = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\input\\mapjoin";
        String out = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\output";
        FileUtils.deleteDirectory(new File(out));

        // 1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar包路径
        job.setJarByClass(DistributedCacheDriver.class);

        // 3 关联map
        job.setMapperClass(DistributedCacheMapper.class);

        // 4 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        // 6 加载缓存数据
        job.addCacheFile(new URI("file:///C:/Users/WJ/Desktop/temp/reduce/input/cache/pd.txt"));

        // 7 map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

        // 8 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
