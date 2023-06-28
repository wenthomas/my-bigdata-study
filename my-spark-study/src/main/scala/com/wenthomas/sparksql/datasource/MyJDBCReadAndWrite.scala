package com.wenthomas.sparksql.datasource

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author Verno
 * @create 2020-03-20 19:28 
 */
/**
 * 从JDBC读写数据：
 * 读数据：
 *      方式一：load方法读取
 *      方式二：read.jdbc方法读取
 * 写数据：
 *      方式一：write.save
 *      方式二：write.jdbc
 */
object MyJDBCReadAndWrite {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyJDBCReadAndWrite").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._

        //--------------------------------------------------------------------------------------------------------
        //(3) 从 jdbc 读数据
        //      方式一：load方法读取
        val jdbcDF = spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/spark")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", "users")
                .load()

        jdbcDF.show()

        //      方式二：read.jdbc方法读取
        //          通过properties设置jdbc连接属性
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "root")

        val jdbcDF2 = spark.read.jdbc("jdbc:mysql://localhost:3306/spark", "users", props)
        jdbcDF2.show()



        //--------------------------------------------------------------------------------------------------------
        //(4) 向 jdbc 写入数据
        //分两种方法: 通用write.save和write.jdbc
        //      方式一：write.save
        val ds = jdbcDF2.as[User]
/*        ds.write
                .format("jdbc")
                .option("url","jdbc:mysql://localhost:3306/spark")
                .option("user","root")
                .option("password","root")
                .option("dbtable","users")
                .mode(SaveMode.Append)
                .save()*/

        //      方式二：write.jdbc
        ds.write
                .mode(SaveMode.Append)
                .jdbc("jdbc:mysql://localhost:3306/spark", "users", props)





        spark.stop()
    }

}

case class User(name: String, age: Long)