package com.xiaozhameng.demo02.demo.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text,
        FlowBean, Text> {
    // 定义一个 TreeMap 作为存储数据的容器（天然按 key 排序）
    private TreeMap<FlowBean, Text> flowMap = new
            TreeMap<FlowBean, Text>();
    private FlowBean kBean;

    @Override
    protected void map(LongWritable key, Text value, Context
            context) throws IOException, InterruptedException {
        kBean = new FlowBean();
        Text v = new Text();
        // 1 获取一行
        String line = value.toString();
        // 2 切割
        String[] fields = line.split("\t");
        // 3 封装数据
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);
        kBean.setDownFlow(downFlow);
        kBean.setUpFlow(upFlow);
        kBean.setSumFlow(sumFlow);
        v.set(phoneNum);
        // 4 向 TreeMap 中添加数据
        flowMap.put(kBean, v);
        // 5 限制 TreeMap 的数据量，超过 10 条就删除掉流量最小的一条数据
        if (flowMap.size() > 10) {
        // flowMap.remove(flowMap.firstKey());
            flowMap.remove(flowMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
// 6 遍历 treeMap 集合，输出数据
        Iterator<FlowBean> bean = flowMap.keySet().iterator();
        while (bean.hasNext()) {
            FlowBean k = bean.next();
            context.write(k, flowMap.get(k));
        }
    }
}