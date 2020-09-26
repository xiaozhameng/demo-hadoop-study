package com.xiaozhameng.demo02.shuffer.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map 阶段
 * <p>
 * KEYIN    输入数据key
 * VALUEIN  输入数据value
 * VALUEIN  输出数据的key的类型
 * KEYOUT   输出数据的value的类型
 * <p> 如下是输入
 * *************************
 * hello java
 * java EE
 * Hadoop Map MapReduce
 * *************************
 * <p> 如下是输出
 * *************************
 * hello 1
 * java 1
 * java 1
 * ****
 * *************************
 * @author xiaozhameng
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 获取一行
        String row = value.toString();

        // 单词分割
        String[] words = row.split(" ");

        // 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
