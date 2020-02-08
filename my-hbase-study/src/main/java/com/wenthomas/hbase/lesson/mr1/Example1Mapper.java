package com.wenthomas.hbase.lesson.mr1;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;

/*
 * 1. MR读取hbase中t2表的部分数据，写入到t4表中
 * 
 * 		Map----Reduce
 * 		读入输入目录中的数据(t2)---map----reduce---写出到指定目录(t4)
 * 
 * 2. 目前读取的是Hbase中的数据，而不是文本文件，所以不能使用默认的TextInputFormat,自然
 * 		RR的泛型无法确定！
 * 
 * 		需要先确定什么样的InputFormat可以读取HBase中的表？
 * 			在hbase中，默认使用TableInputFormat输入格式
 * 					切片： 一个region切一片
 * 					RR： RecordReader<ImmutableBytesWritable, Result>
 * 							ImmutableBytesWritable： rowkey
 * 							Result: 一行数据
 * 
 * 3. 输出的数据应该是一个put对象！
 * 			在Maper中,数据如果需要排序，必须作为key，如果不需要排序可以作为value
 * 
 * 4. 如果Mapper输出的value是put类型，会自动设置Combiner!
 * 			无法避免设置Combiner时，可以让Combiner的逻辑无法执行！
 * 			保证输出的key不能相等！
 * 
 * 5. Hadoop MR处理的数据必须都是key-value
 * 			在hadoop中，只要有Reducer，那么此时Key-value必须实现序列化！
 * 			Wriable：  key实现WriableComparable
 * 					value实现Wriable
 * 
 * 		为什么Put没有实现Wriable，程序依然没有报错？
 * 			因为：hbase自动提供了对Put类型的序列化器！
 *   conf.setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName());
 * 
 * 			之前为什么强调key-value都必须实现Wriable，是因为key-value实现了Writable，
 * 			hadoop为实现了Wriable接口的类，提供了序列化器！
 * 
 * 		在hadoop中，必须提供序列化器！至于类型是否实现序列化，是无关紧要的！
 * 
 * 		
 * 
 * 
 */
public class Example1Mapper extends TableMapper<Text, Put>{
	
	private Text out_key=new Text();
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {
		
		out_key.set(Bytes.toString(key.copyBytes()));
		
		//构建Put对象,封装rowkey
		Put put = new Put(key.copyBytes());
		
		Cell[] rawCells = value.rawCells();
		
		for (Cell cell : rawCells) {
			
			//讲读到的数据，进行过滤，只选择info:age=20的行的内容进行输出
			if (Bytes.toString(CellUtil.cloneFamily(cell)).equals("info") 
					&& Bytes.toString(CellUtil.cloneQualifier(cell)).equals("age")
					&& Bytes.toString(CellUtil.cloneValue(cell)).equals("20")) {
				
				//把复合要求的一行的所有单元格放入到put对象中
				for (Cell c : rawCells) {
					
					put.add(c);
					
				}
				
				//把整个put写出
				context.write(out_key, put);
				
			}
			
		}
		
	}

}
