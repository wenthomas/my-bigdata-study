package com.wenthomas.hbase.lesson.mr2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Example2Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//创建一个Job的configuration
		Configuration conf = HBaseConfiguration.create();
		//创建Job
		Job job = Job.getInstance(conf);
		
		job.setJobName("example2");
		
		job.setJarByClass(Example2Driver.class);
		
		//设置Job使用的Mapper的类型
		job.setMapperClass(Example2Mapper.class);
		//设置Job使用的Mapper的输出的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Put.class);
		// 设置输入目录
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:9000/hive/movie_info/data2"));
		
		//配置Job Reducer的设置
		TableMapReduceUtil.initTableReducerJob("t5", Example2Reducer.class, job);
		
		
		//运行
		job.waitForCompletion(true);
		
	}
	

}
