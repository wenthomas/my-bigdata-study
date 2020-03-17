package com.wenthomas.sparkcore.jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-17 10:25 
 */

/**
 * 注意：
 * 1，sql必须提供占位符，并且prepareStatement中需要指定各字段对应的columnIndex
 * 2，为防止频繁建立连接造成mysql压力过大，应使用foreachPartition按分区创建连接，不能用foreach创建连接
 */
object MyDBWrite extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("MyDBCRead").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark"
    val userName = "root"
    val passWd = "root"
    val sql = "insert into users values(?,?)"

    //需要插入的数据
    private val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("Ronaldo", 34), ("Beckham", 40), ("Robin", 38)))

    //1，按照分区建立连接
    rdd.foreachPartition(it => {
        Class.forName(driver)
        //获取连接
        val conn = DriverManager.getConnection(url, userName, passWd)

        //执行sql
        val ps = conn.prepareStatement(sql)

        var count = 0

        //2，按照批次进行写入
        it.foreach({
            case (name, age) =>
                ps.setString(1, name)
                ps.setInt(2, age)
                ps.addBatch()
                count += 1
                if (count >= 100) ps.executeBatch()
        })

        ps.executeBatch()
        conn.close()
    })

    sc.stop()

}
