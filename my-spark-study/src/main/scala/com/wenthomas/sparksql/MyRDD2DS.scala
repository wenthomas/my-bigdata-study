package com.wenthomas.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Verno
 * @create 2020-03-20 14:17 
 */
/**
 * RDD 与 DS的转换：
 *      rdd -> ds: rdd.toDS   注意：需要import spark.implicits._
 *      ds -> rdd: ds.rdd     注意：不需要导入隐式转换
 */
object MyRDD2DS {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyCreateDS").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val rdd = spark.sparkContext.makeRDD(Array(User("zhangsan",10),User("lisi",20),User("wangwu",15)))
        //--------------------------------------------------------------------------------------------------------
        //(1) RDD 转成DS: rdd.toDS
        import spark.implicits._
        val ds = rdd.toDS
        //--------------------------------------------------------------------------------------------------------
        //(2) DS转RDD
        val rdd1 = ds.rdd
        rdd1.foreach(println)

        //4，关闭SparkSession
        spark.close()
    }
}
