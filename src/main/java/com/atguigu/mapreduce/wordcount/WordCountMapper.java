package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: HDFSClient
 * @BelongsPackage: com.atguigu.mapreduce.wordcount
 * @Author: qzk
 * @CreateTime: 2023/10/10 17:43
 * @Description: TODO
 * @Version: 1.0
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    //声明在外面，节省空间
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    /**
     * KEYIN, map阶段输入的key的类型：LongWritable 记录偏移量 按照行记录
     * VALUEIN,map阶段输入value类型：Text   记录具体内容
     * KEYOUT,map阶段输出的Key类型：Text
     * VALUEOUT,map阶段输出的value类型：IntWritable
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //map是 每个kv对（即每行内容） 都执调用一次
        String line = value.toString();

        String[] words = line.split(" ");

        for (String word : words) {
            outK.set(word);
            context.write(outK,outV);
        }
    }
}
