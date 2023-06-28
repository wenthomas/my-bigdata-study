package com.wenthomas.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Verno
 * @create 2020-03-20 11:19 
 */

/**
 * 创建DF
 */
object MyCreateDF {

    def main(args: Array[String]): Unit = {
        //1，创建SparkSession
        val conf = new SparkConf().setAppName("MyCreateDF").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        //2，通过SparkSession创建DF
        val df = spark.read.json("file:///E:/1015/my_data/1.json")

        //创建临时视图
        df.createOrReplaceTempView("user")

        //使用Spark SQL查询
        spark.sql(
            """
                |select *
                |from user
                |where name = 'wenthomas'
                |""".stripMargin
        ).show

        //3，对DF进行操作（SQL）

        //4，关闭SparkSession
        spark.close()
    }

}
