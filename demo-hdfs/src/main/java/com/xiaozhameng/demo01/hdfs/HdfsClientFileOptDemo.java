package com.xiaozhameng.demo01.hdfs;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * hdfs文件操作
 */
public class HdfsClientFileOptDemo {

    private FileSystem fs;

    @Test
    public void fileSeek() throws Exception {
        FSDataInputStream fis = fs.open(new Path("/jdk-8u251-linux-x64.tar.gz"));
        FileOutputStream fos = new FileOutputStream("C:\\Users\\WJ\\Desktop\\temp\\file\\jdk-tar.gz.part1");
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(bytes);
            fos.write(bytes);

        }
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        System.out.println("完成第一部分文件拷贝");

        TimeUnit.SECONDS.sleep(2);

        fis = fs.open(new Path("/jdk-8u251-linux-x64.tar.gz"));
        fos = new FileOutputStream("C:\\Users\\WJ\\Desktop\\temp\\file\\jdk-tar.gz.part2");

        fis.seek(1024 * 1024 * 128);
        IOUtils.copyBytes(fis,fos,new Configuration());
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        System.out.println("完成第二部分文件拷贝");
    }

    @Test
    public void testFileIO() throws IOException {
        FileInputStream fis = new FileInputStream(new File("C:\\Users\\WJ\\Desktop\\temp\\file\\ssss.txt"));
        FSDataOutputStream out = fs.create(new Path("/demo/io/copy.txt"));
        IOUtils.copyBytes(fis, out, new Configuration());
        System.out.println("文件流操作文件上传完成");

        FSDataInputStream open = fs.open(new Path("/demo/io/copy.txt"));
        IOUtils.copyBytes(open, new FileOutputStream("C:\\Users\\WJ\\Desktop\\temp\\file\\ssss-copy.txt"), new Configuration());
        System.out.println("我又把文件下载下来了！！");
    }

    @Test
    public void testFileList() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            System.out.println("数据块 --> " + next);

            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("block-location： " + blockLocation);
            }

        }

    }

    @Test
    public void testCopyToLocal() throws Exception {
        // 文件上传操作
        fs.copyToLocalFile(new Path("/demo/filecopy1/abc.txt"), new Path("C:\\Users\\WJ\\Desktop\\temp\\file\\ttt.txt"));
        System.out.println("文件下载完成");
    }


    @Test
    public void testCopyFromLocal() throws Exception {
        // 文件上传操作
        fs.copyFromLocalFile(new Path("C:\\Users\\WJ\\Desktop\\temp\\file\\abc.txt"),
                new Path("/demo/filecopy1/abc.txt"));
        System.out.println("文件上传完成");
    }


    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
        System.out.println("============= 开始测试，资源初始化 =============");
    }

    @After
    public void destroy() throws IOException {
        fs.close();
        System.out.println("============= 测试结束，资源已释放 =============");
    }
}
