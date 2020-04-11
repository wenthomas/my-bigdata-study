package com.wenthomas.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Verno
 * @create 2020-03-20 14:28 
 */
/**
 * DF 和 DS 之间的转换：
 * (1) df -> ds：df.as[样例类]
 * (2) ds -> df： ds.toDF
 *
 */
object MyDF2DS {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyDF2DS").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val rdd = spark.sparkContext.makeRDD(Array(User("zhangsan",10),User("lisi",20),User("wangwu",15)))

        import spark.implicits._
        //--------------------------------------------------------------------------------------------------------
        //(1) df -> ds：df.as[样例类]
        /**
         * 注意：
         *      1)需要导入隐式转换
         *      2)需要映射到一个样例类
         *      3)从文件中读到的数据类型默认为BigInt，在调用df.as时注意类型转换，样例类对应的属性应设为Long
         */
        val df = spark.read.json("file:///E:/1015/my_data/1.json")
        val ds = df.as[User1]
        //--------------------------------------------------------------------------------------------------------
        //(2) ds -> df： ds.toDF
        /**
         * 注意：
         *      不需要导入隐式转换
         */
        val df1 = ds.toDF
        //--------------------------------------------------------------------------------------------------------

        spark.stop()
    }

}

case class User1(name: String, age: Long)