package com.wenthomas.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-02-07 16:27
 */
public class MyRunner {
    private static Logger logger= LoggerFactory.getLogger(MyRunner.class);

    public static void main(String[] args) {
        Connection conn = null;

        try {
            //获取Connection对象进行hbase api操作
            conn = ConnectionUtil.getConn();

            //测试方法

            //NameSpaceUtil.ifNSExist(conn, "default");

/*            for (String listNameSpace : NameSpaceUtil.listNameSpaces(conn)) {
                logger.info("NameSpace={}",listNameSpace);
            }*/

/*            for (String table : NameSpaceUtil.getTablesInNameSpace(conn, "default")) {
                logger.info("NameSpace={}",table);
            }*/

//            NameSpaceUtil.createNS(conn, "api");

//            logger.info(NameSpaceUtil.deleteNS(conn, "api") + "");

//            TableUtil.createTable(conn, "api_test","default", "cf1", "cf2");

//            logger.info("table is exist: {}", TableUtil.ifTableExists(conn, "student", "default"));

//            TableUtil.dropTable(conn, "api_test","default");

//            DataUtil.put(conn, "student", "default", "1003", "info", "name", "Thomas");

            DataUtil.get(conn, "student", "default", "1003");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //关闭Connection连接
            try {
                if (null != conn) {
                    conn.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
