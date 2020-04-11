package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 14:08 
 */
object MyGlom extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,2,3,4,5,6,7,8,9)
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //glom()：将每个分区的数据分别合并为一个数组，最后再将各数组合并为一个大数组
    //注意：分区内数据过大时可能会OOM，因为分区数据算完之前不会释放内存
    private val glomRDD = rdd.glom().map(x => x.toList)


    //4，行动算子
    private val result = glomRDD.collect

    //List(1, 2, 3),List(4, 5, 6),List(7, 8, 9)
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
