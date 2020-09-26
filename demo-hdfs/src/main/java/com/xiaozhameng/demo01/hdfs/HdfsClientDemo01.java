package com.xiaozhameng.demo01.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientDemo01 {

    /**
     * <property>
     * <name>fs.defaultFS</name>
     *  <value>hdfs://hadoop102:9000</value>
     * </property>
     *
     * 指定HADOOP_USER 参数 -DHADOOP_USER_NAME=atguigu
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop102:9000");

        // 获取资源 -DHADOOP_USER_NAME=atguigu
        // FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        // 执行操作
        fs.mkdirs(new Path("/hadoop/demo"));

        // 释放资源
        fs.close();

        System.out.println("执行完毕！");
    }
}
