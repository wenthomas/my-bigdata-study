package com.wenthomas.hbase.lesson.mr1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Example1Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//创建一个Job的configuration
		Configuration conf = HBaseConfiguration.create();
		//创建Job
		Job job = Job.getInstance(conf);
		
		job.setJobName("example1");
		
		job.setJarByClass(Example1Driver.class);
		
		Scan scan = new Scan();
		
		//配置Job Mapper的设置
		TableMapReduceUtil.initTableMapperJob("t2", scan, Example1Mapper.class,
				Text.class, Put.class, job);
		
		//配置Job Reducer的设置
		TableMapReduceUtil.initTableReducerJob("t4", Example1Reducer.class, job);
		
		
		//运行
		job.waitForCompletion(true);
		
	}
	

}
