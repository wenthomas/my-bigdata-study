package com.wenthomas.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-13 10:18 
 */
object CreateRDD extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(10,20,30,40,50,60)
    //方式一：parallelize()创建
    //private val rdd: RDD[Int] = sc.parallelize(arr)

    //方式二：mkRDD()创建（常用）
    private val rdd: RDD[Int] = sc.makeRDD(arr)

    //方式三：从外部存储中创建
    private val rdd1: RDD[String] = sc.textFile("file://E:/1015/my_data/mr/hello.txt")

    //3，转换

    //4，行动算子
    private val result: Array[Int] = rdd.collect


    println(result.mkString(","))

    //5，关闭连接
    sc.stop()
}
