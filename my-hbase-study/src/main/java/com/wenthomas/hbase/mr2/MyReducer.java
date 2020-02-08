package com.wenthomas.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * @author Verno
 * @create 2020-02-08 18:51
 */

/*
 * 1. Reducer输出的类型必须是Mutation，是固定的！
 * 			Mutation是所有 写数据类型的父类！
 *
 * Reducer的任务就是将Mapper输出的所有记录写出
 */
public class MyReducer extends TableReducer<Text, Put, Text> {
}
