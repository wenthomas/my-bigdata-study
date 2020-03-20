package com.wenthomas.sparksql.datasource

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author Verno
 * @create 2020-03-20 16:48 
 */

/**
 * 通用读写数据源方法：
 *      通用读：spark.read.format("文件格式类型").load(文件路径)
 */
object MyCommonReadAndWrite {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyCommonReadAndWrite").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        //--------------------------------------------------------------------------------------------------------
        //(1) 通用读写json文件
        //      读
        // spark.read.format("json").load(文件路径) 或者 spark.read.json(文件路径)
        val df = spark.read.format("json").load("file:///E:/1015/my_data/1.json")

        val df1 = spark.read.json("file:///E:/1015/my_data/1.json")
        df1.show()

        //      直接在json文件上执行SQL查询
        spark.sql(
            """
                |select * from json.`file:///E:/1015/my_data/1.json`
                |""".stripMargin).show()

        //      写




        //--------------------------------------------------------------------------------------------------------
        //(2) 通用读parquet文件（默认）
        //      读
        //      写
        df.write.mode(SaveMode.Overwrite).parquet("output/user.parquet")





        spark.stop()
    }

}
