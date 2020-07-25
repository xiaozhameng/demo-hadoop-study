在企业开发中，Hadoop 框架自带的InputFormat 类型不可能满足所有应用场景，需要自定义InputFormat 来解决实际问题

自定义InputFormat 的步骤如下
1、自定义一个类，继承InputFormat
2、改写RecordReader，实现一次读取一个完整文件，封装成KV
3、在输出时使用SequenceFileOutPutFormat 输出合并文件

这里的例子就是以合并小文件为例

一、需求
将多个小文件合并成一个SequenceFile 文件（SequenceFile 文件是Hadoop 用来存储二进制形式的key-value对的文件格式）。SequenceFile 里面存放
多个文件，存储形式是文件路径+名称为key，文件内容为value

# 数据输入
    one.txt
    two.txt
    three.txt
# 数据输出
    part-r-00000