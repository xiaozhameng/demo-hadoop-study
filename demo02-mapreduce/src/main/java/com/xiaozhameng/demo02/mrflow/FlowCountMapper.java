package com.xiaozhameng.demo02.mrflow;

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
public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 1、获取一行
        // 1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
        String rowData = value.toString();

        // 2、数据分割
        String[] fields = rowData.split("\t");

        // 3、组装数据
        // 获取手机号，封装成key
        String phoneNo = fields[1];
        // 上行，下行，总流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        k.set(phoneNo);
        v.set(upFlow,downFlow);

        // 4、写出数据
        context.write(k,v);
    }

}
