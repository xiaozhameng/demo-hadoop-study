package com.xiaozhameng.demo02.inputformat.cust;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 功能描述：编写reducer 处理逻辑
 *
 * @author: xiaozhameng
 * @date: 2020/7/23 1:27 下午
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text,BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,values.iterator().next());
    }
}
