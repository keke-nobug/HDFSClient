package com.atguigu.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @BelongsProject: HDFSClient
 * @BelongsPackage: com.atguigu.writable
 * @Author: qzk
 * @CreateTime: 2023/10/11 18:22
 * @Description: TODO
 * @Version: 1.0
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\EnvironmentTools\\JetBrains\\IDEAWorkSpace\\HDFSClient\\src\\main\\java\\com\\atguigu\\writable\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\EnvironmentTools\\JetBrains\\IDEAWorkSpace\\HDFSClient\\src\\main\\java\\com\\atguigu\\writable\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
