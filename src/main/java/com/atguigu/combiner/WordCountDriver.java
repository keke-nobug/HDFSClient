package com.atguigu.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @BelongsProject: HDFSClient
 * @BelongsPackage: com.atguigu.mapreduce.wordcount
 * @Author: qzk
 * @CreateTime: 2023/10/10 17:43
 * @Description: TODO
 * @Version: 1.0
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountCombiner.class);


        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //本地Windows环境下执行
        FileInputFormat.setInputPaths(job, new Path("D:\\EnvironmentTools\\JetBrains\\IDEAWorkSpace\\HDFSClient\\src\\main\\java\\com\\atguigu\\mapreduce\\wordcount\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\EnvironmentTools\\JetBrains\\IDEAWorkSpace\\HDFSClient\\src\\main\\java\\com\\atguigu\\combiner\\output3"));

        //打包到linux环境下执行：hadoop jar HDFSClient-1.0-SNAPSHOT.jar com/atguigu/mapreduce/wordcount/WordCountDriver  或者设置成args参数，灵活输入输出路径
//        FileInputFormat.setInputPaths(job, new Path("/input"));
//        FileOutputFormat.setOutputPath(job, new Path("/output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
