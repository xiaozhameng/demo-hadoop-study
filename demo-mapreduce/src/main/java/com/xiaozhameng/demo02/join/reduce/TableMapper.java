package com.xiaozhameng.demo02.join.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    Text k = new Text();
    TableBean v = new TableBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 获取输入文件类型
        FileSplit split = (FileSplit)context.getInputSplit();
        String name = split.getPath().getName();
        System.out.println("文件类型 = " + name);

        // 获取一行数据
        String line = value.toString();

        if (name.startsWith("order")){
            // 订单表 id	pid	amount
            String[] fields = line.split("\t");
            v.setOrder_id(fields[0]);
            v.setP_id(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("0");
        }
        else {
            // 产品表 pid	pname
            String[] fields = line.split("\t");
            v.setOrder_id("");
            v.setP_id(fields[0]);
            v.setAmount(0);
            v.setPname(fields[1]);
            v.setFlag("1");
        }

        // 通过将关联条件作为map输出的key
        k.set(v.getP_id());

        context.write(k,v);
    }
}
