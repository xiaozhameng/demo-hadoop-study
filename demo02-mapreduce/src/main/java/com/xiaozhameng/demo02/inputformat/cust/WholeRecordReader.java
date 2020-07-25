package com.xiaozhameng.demo02.inputformat.cust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 功能描述：自定义RecordReader
 *
 * @author: xiaozhameng
 * @date: 2020/7/23 1:09 下午
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private Text k = new Text();
    private BytesWritable v = new BytesWritable();
    private boolean isProgress = true;

    private FileSplit split;
    private Configuration configuration;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        this.split= (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress){
            FSDataInputStream fis = null;
            // 定义缓冲区
            byte[] contents = new byte[(int)split.getLength()];
            try {
                // 获取文件系统
                Path path = split.getPath();
                FileSystem fs = path.getFileSystem(configuration);

                // 读取数据
                fis = fs.open(path);

                // 读取文件内容
                IOUtils.readFully(fis,contents,0,contents.length);

                // 输出文件内容
                v.set(contents,0,contents.length);

                // 文件路径以及名称
                String name = split.getPath().toString();

                // 设置输出的key
                k.set(name);

            }catch (Exception e){

            }finally {
                IOUtils.closeStream(fis);
            }
            isProgress = false ;
            return true ;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}
