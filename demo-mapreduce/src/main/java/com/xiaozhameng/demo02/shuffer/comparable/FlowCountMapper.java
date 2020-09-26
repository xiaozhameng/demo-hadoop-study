package com.xiaozhameng.demo02.shuffer.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper类
 *
 * 获取一行
 * @author xiaozhameng
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 数据  12776520515  2481    24681  27162
        String line = value.toString();

        String[] fields = line.split("\t");

        String phoneNo = fields[0];
        // 上行，下行，总流量
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        k.set(upFlow,downFlow);
        v.set(phoneNo);

        context.write(k,v);
    }
}
