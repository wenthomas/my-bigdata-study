package com.wenthomas.hbase.lesson;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/*
 * 1.创建和关闭Connection对象
 * 
 * 2.如何在HBase中创建一个Configuration对象
 * 		可以使用HBaseConfiguration.create()，返回的Configuration,既包含
 * 		hadoop8个配置文件的参数，又包含hbase-default.xml和hbase-site.xml中所有的参数配置！
 */
public class ConnectionUtil {
	
	//创建一个Connection对象
	public static Connection getConn() throws IOException {
		
		return ConnectionFactory.createConnection();
		
	}
	
	
	public static void close(Connection conn) throws IOException {
		
		if (conn !=null) {
			conn.close();
		}
		
	}
	
	

}
