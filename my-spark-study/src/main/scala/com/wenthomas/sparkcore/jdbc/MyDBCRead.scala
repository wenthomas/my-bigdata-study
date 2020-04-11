package com.wenthomas.sparkcore.jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-17 10:03 
 */

/**
 * 注意：
 * 1，sql需要传入查询上下限，尽量该上下限字段的类型为数值型，否则sql中的<= 和 >=无法正确分区导致数据倾斜
 */
object MyDBCRead extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("MyDBCRead").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark"
    val userName = "root"
    val passWd = "root"

    /**
     * 数据：
     * (wenthomas,25)
     * (tom,26)
     * (thomas,27)
     * (verno,25)
     * (test,26)
     */



    private val rdd = new JdbcRDD(
        sc,
        () => {
            //建立到mysql的连接
            //1，加载驱动
            Class.forName(driver)
            //2，获取连接
            DriverManager.getConnection(url, userName, passWd)
        },
        "select * from users where age >= ? and age <= ?",
        20,
        25,
        2,
        result => (result.getString(1), result.getInt(2))
    )

    //(wenthomas,25)
    //(verno,25)
    rdd.collect.foreach(println)


    //关闭连接
    sc.stop()

}
