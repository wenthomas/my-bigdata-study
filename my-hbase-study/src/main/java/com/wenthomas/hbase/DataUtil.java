package com.wenthomas.hbase;

import javafx.geometry.VPos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-02-07 16:21
 */

/*
 * 1.数据的增删改查，需要使用的是Table
 *
 * 2.Put: 代表对单行数据的put操作
 *
 * 3. 在hbase中，操作的数据都是以byte[]形式存在，需要把常用的数据类型转为byte[]
 * 			hbase提供了Bytes工具类
 * 				Bytes.toBytes(x): 基本数据类型转byte[]
 * 				Bytes.toXxx(x): 从byte[]转为Xxx类型！
 *
 * 4. Get: 代表对单行数据的Get操作！
 *
 * 5. Result: scan或get的单行的所有的记录！
 *
 * 6. Cell： 代表一个单元格，hbase提供了CellUtil.clonexxx(Cell)，来获取cell中的列族、列名和值属性！
 */
public class DataUtil {
    private static Logger logger= LoggerFactory.getLogger(DataUtil.class);

    /**
     * 操作数据的前提是要先获取Table对象
     * @param conn
     * @param tableName
     * @param nsname
     * @return
     * @throws IOException
     */
    public static Table getTable(Connection conn, String tableName, String nsname) throws IOException {
        //验证表名是否合法
        TableName tn = TableUtil.checkTableName(tableName, nsname);

        if (null == tn) {
            logger.error("表名不合法！");
            return null;
        }

        //根据TableName获取对应的Table
        return conn.getTable(tn);
    }

    /**
     * 插入/修改数据
     * @param conn
     * @param tableName
     * @param nsname
     * @param rowkey
     * @param cf 列族名
     * @param cq 具体列名
     * @param value 值
     */
    public static void put(Connection conn,String tableName,String nsname,String rowkey,String cf,
                           String cq,String value) throws IOException {
        //获取表对象
        Table table = getTable(conn, tableName, nsname);

        if (table==null) {
            return;
        }

        //1, 创建一个Put对象：指定要插入的行
        Put put = new Put(Bytes.toBytes(rowkey));

        //2, 向put中设置cell的具体信息
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(value));

        //3, 插入数据
        table.put(put);

        logger.info("成功插入一条数据");
        //4, 关闭table
        table.close();
    }

    /**
     * 查询具体某行数据
     * @param conn
     * @param tableName
     * @param nsname
     * @param rowkey
     */
    public static void get(Connection conn,String tableName,String nsname,String rowkey) throws IOException {
        //0, 获取Table对象
        Table table = getTable(conn, tableName, nsname);

        if (table==null) {
            return ;
        }

        //1, 新建Get对象
        Get get = new Get(Bytes.toBytes(rowkey));

        //2, 设置单行查询的详细信息
        //设置查哪个列
        //get.addColumn(family, qualifier)
        //设置查哪个列族
        //get.addFamily(family)
        //只查某个时间戳的数据
        //get.setTimeStamp(timestamp)
        //设置返回的versions
        //get.setMaxVersions(maxVersions)

        //3, 查询获取Result对象
        Result result = table.get(get);

        //4, 遍历解析
        parseResult(result);

        logger.info("成功查询一条数据！");
        //5, 关闭table
        table.close();
    }

    /**
     * 遍历解析输出result对象
     * @param result
     */
    public static void parseResult(Result result) {
        if (null != result) {
            //1，获取所有cell集合
            Cell[] cells = result.rawCells();

            //2, 遍历集合输出查询结果
            for (Cell cell : cells) {
                System.out.println("行："+Bytes.toString(CellUtil.cloneRow(cell))+
                        "  列族："+Bytes.toString(CellUtil.cloneFamily(cell))+"   列名："+
                        Bytes.toString(CellUtil.cloneQualifier(cell))+
                        "  值:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
