package com.wenthomas.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-02-08 18:03
 */

/**
 * 注意：IDE运行hbase mr时记得把hadoop相关依赖都去掉，否则可能会由于依赖冲突出错：org.apache.hadoop.yarn.api.records.LocalResource.setShouldBeUploadedToSharedCache(Z)V
 */
public class MyDriver {
    private static Logger logger= LoggerFactory.getLogger(MyDriver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建一个Job的configuration
        Configuration conf = HBaseConfiguration.create();

        //创建Job
        Job job = Job.getInstance(conf);

        //设置Job属性参数
        job.setJobName("mr1");
        job.setJarByClass(MyDriver.class);

        Scan scan = new Scan();

        //配置Mapper：TableMapReduceUtil
        TableMapReduceUtil.initTableMapperJob("mr1_1", scan, MyMapper.class, Text.class, Put.class, job);

        //配置Reducer：TableMapReduceUtil
        TableMapReduceUtil.initTableReducerJob("mr1_2", MyReducer.class, job);

        //运行Job
        job.waitForCompletion(true);
    }
}
