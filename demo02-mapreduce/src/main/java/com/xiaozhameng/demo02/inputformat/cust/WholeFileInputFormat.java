package com.xiaozhameng.demo02.inputformat.cust;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 功能描述：自定义FileInputFormat
 *
 * @author: xiaozhameng
 * @date: 2020/7/23 1:05 下午
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false ;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}
