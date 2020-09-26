默认就是Text的类型的

1、TextInputFormat 是默认的的FileInputFormat 实现
2、默认按行读取每条记录，键是存储该行在整个文件中的起始字节偏移量，类型为LongWritable
3、值是这行的内容，不包括任何终止符（换行和回车符号都没有）
