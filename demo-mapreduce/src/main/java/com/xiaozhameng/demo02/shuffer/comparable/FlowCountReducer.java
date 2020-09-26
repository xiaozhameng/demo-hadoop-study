package com.xiaozhameng.demo02.shuffer.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 * @author xiaozhameng
 */
public class FlowCountReducer extends Reducer<FlowBean,Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
