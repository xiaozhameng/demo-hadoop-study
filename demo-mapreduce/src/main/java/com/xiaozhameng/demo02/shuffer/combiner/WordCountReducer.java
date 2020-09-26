package com.xiaozhameng.demo02.shuffer.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 *
 * Map 阶段的输出作为Reduce阶段的输入
 *
 * <p> 如下是Map阶段的输出
 * *************************
 * hello 1
 * java 1
 * @author xiaozhameng
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    int sum = 0;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
