package com.xiaozhameng.demo02.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class CompressTest {

    public static void main(String[] args) throws Exception {
        String file = "C:\\Users\\WJ\\Desktop\\temp\\reduce\\input\\compress\\eula.1028.txt";
        String compressType = "org.apache.hadoop.io.compress.BZip2Codec";
        compressType = "org.apache.hadoop.io.compress.DefaultCodec";
        compressType = "org.apache.hadoop.io.compress.GzipCodec";

        // 统计
        compress(file,compressType);
        // 解压缩
        deCompress(file + ".gz");
    }

    private static void deCompress(String file) throws IOException {
        // 获取输入流

        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(file));

        if (codec == null){
            System.out.println("找不到对应的编解码器");
        }

        CompressionInputStream input = codec.createInputStream(new FileInputStream(file));
        // （2）获取输出流
        FileOutputStream fos = new FileOutputStream(new File(file + ".decoded"));

        // 写出
        IOUtils.copyBytes(input,fos,1024*1024,true);
    }

    private static void compress(String file,String compressType) throws IOException, ClassNotFoundException {
        // 获取输入流
        FileInputStream fis = new FileInputStream(file);

        Class<?> aClass = Class.forName(compressType);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(aClass, conf);

        // 获取输出流
        String res = file + codec.getDefaultExtension();
        FileOutputStream fos = new FileOutputStream(res);
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis,cos,1024*1024*5,true);
    }
}
