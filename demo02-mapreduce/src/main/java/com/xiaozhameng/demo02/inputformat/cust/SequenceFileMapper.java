package com.xiaozhameng.demo02.inputformat.cust;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 功能描述：编写Mapper 处理流程
 *
 * @author: xiaozhameng
 * @date: 2020/7/23 1:25 下午
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text,BytesWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
