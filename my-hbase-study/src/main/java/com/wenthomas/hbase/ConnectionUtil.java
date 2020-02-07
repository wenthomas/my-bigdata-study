package com.wenthomas.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-02-07 16:21
 */

/*
 * 1.创建和关闭Connection对象
 *
 * 2.如何在HBase中创建一个Configuration对象
 * 		可以使用HBaseConfiguration.create()，返回的Configuration,既包含
 * 		hadoop8个配置文件的参数，又包含hbase-default.xml和hbase-site.xml中所有的参数配置！
 */
public class ConnectionUtil {
    /**
     * 创建一个Connection对象
     * @return
     */
    public static Connection getConn() throws IOException {
        return ConnectionFactory.createConnection();
    }

    public static void closeConn(Connection conn) throws IOException {
        if (null != conn) {
            conn.close();
        }
    }
}
