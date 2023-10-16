package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @BelongsProject: HDFSClient
 * @BelongsPackage: com.atguigu.hdfs
 * @Author: qzk
 * @CreateTime: 2023/10/9 19:42
 * @Description: TODO
 * @Version: 1.0
 */

/**
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS  zookeeper
 */
public class HDFSClient {

    private FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        // 用户
        String user = "qzk";

        // 1 获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }
    @After
    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }
    @Test
    public void testMkdir() throws IOException {
        // 2 相关操作，创建一个文件夹
        fs.mkdirs(new Path("/xiyouji/huaguoshan1"));
    }
    @Test
    public void testPut() throws IOException {
        //参数解读：参数一：表示删除原数据； 参数二：是否允许覆盖；参数三：原数据路径； 参数四：目的地路径
        //最后一个参数目标路径，加不加 头hdfs://hadoop102 都可以
        fs.copyFromLocalFile(false,true,new Path("D:\\StudyData\\尚硅谷大数据技术之Hadoop3.x\\代码\\sunwukong.txt"),new Path("/xiyouji/huaguoshan"));
    }
    @Test
    public void testGet() throws IOException {
        fs.copyToLocalFile(false,new Path("/xiyouji/huaguoshan/"),new Path("D:\\"),false);
    }

    @Test
    public void testRm() throws IOException {
        fs.delete(new Path("/xiyouji/huaguoshan1"),true);
    }

    @Test
    public void testMv() throws IOException {
        fs.rename(new Path("/output1"),new Path("/output2"));
    }

    @Test
    public void fileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        // 遍历文件
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();

            System.out.println("==========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }
    }

    @Test
    public void testCheckFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {

            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            } else {
                System.out.println("目录：" + status.getPath().getName());
            }
        }
    }
}



