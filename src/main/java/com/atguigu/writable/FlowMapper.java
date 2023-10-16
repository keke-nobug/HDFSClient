package com.atguigu.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

/**
 * @BelongsProject: HDFSClient
 * @BelongsPackage: com.atguigu.writable
 * @Author: qzk
 * @CreateTime: 2023/10/11 15:19
 * @Description: TODO
 * @Version: 1.0
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];

        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upFlow));
        outV.setDownFlow(Long.parseLong(downFlow));
        outV.setSumFlow();

        context.write(outK,outV);

    }
}
