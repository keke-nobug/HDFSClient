package com.atguigu.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: HDFSClient
 * @BelongsPackage: com.atguigu.mapreduce.wordcount
 * @Author: qzk
 * @CreateTime: 2023/10/10 17:43
 * @Description: TODO
 * @Version: 1.0
 */

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable outV = new IntWritable();

    //每个key调用一次该方法
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value :
                values) {
            sum += value.get();
        }
        outV.set(sum);
        context.write(key,outV);
    }
}