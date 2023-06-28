package com.wenthomas.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-02-08 18:47
 */

/*
 * 1. MR读取HDFS中的数据，写入到hbase表中
 *
 * 		InputFormat无需使用TableInputFormat!
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, Put> {
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] str = value.toString().split("\t");

        //封装rowkey列
        k.set(str[0]);

        Put put = new Put(Bytes.toBytes(str[0]));

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("categery"), Bytes.toBytes(str[1]));

        context.write(k, put);
    }
}
