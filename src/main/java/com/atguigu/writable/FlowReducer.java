package com.atguigu.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

/**
 * @BelongsProject: HDFSClient
 * @BelongsPackage: com.atguigu.writable
 * @Author: qzk
 * @CreateTime: 2023/10/11 18:16
 * @Description: TODO
 * @Version: 1.0
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long totalUpFlow = 0;
        long totaldownFlow = 0;

        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totaldownFlow += value.getDownFlow();
        }

        outV.setUpFlow(totalUpFlow);
        outV.setDownFlow(totaldownFlow);
        outV.setSumFlow();

        context.write(key,outV);
    }
}
