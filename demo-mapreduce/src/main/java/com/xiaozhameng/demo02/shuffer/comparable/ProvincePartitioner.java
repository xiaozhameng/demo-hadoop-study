package com.xiaozhameng.demo02.shuffer.comparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {
        // key 是手机号
        // value 是流量信息
        String prePhoneNum = value.toString().substring(0, 3);
        int partition = 4;
        if ("136".equals(prePhoneNum)) {
            partition = 0;
        } else if ("137".equals(prePhoneNum)) {
            partition = 1;
        } else if ("138".equals(prePhoneNum)) {
            partition = 2;
        } else if ("139".equals(prePhoneNum)) {
            partition = 3;
        }
        return partition;
    }


//
//    @Override
//    public int getPartition(Text key, FlowBean value, int numPartitions) {
//        // key 是手机号
//        // value 是流量信息
//        String prePhoneNum = key.toString().substring(0, 3);
//        int partition = 4;
//        if ("136".equals(prePhoneNum)){
//            partition = 0;
//        }else if ("137".equals(prePhoneNum)){
//            partition = 1;
//        }else if ("138".equals(prePhoneNum)){
//            partition = 2;
//        }else if ("139".equals(prePhoneNum)){
//            partition = 3;
//        }
//        return partition;
//    }
}
