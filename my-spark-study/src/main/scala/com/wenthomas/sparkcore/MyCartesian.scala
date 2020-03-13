package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 16:51 
 */
object MyCartesian extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,1,2,3,4,5,6,7,8,9)
    val arr1 = Array(43,52,556,23,1,3)
    private val rdd= sc.makeRDD(arr,3)
    private val rdd1: RDD[Int] = sc.makeRDD(arr1)

    //3，转换
    //cartesian():求笛卡尔积
    private val cartesianRDD = rdd.cartesian(rdd1)

    //4，行动算子
    private val result = cartesianRDD.collect

    // 结果(1,43),(1,43),(2,43),(1,52),(1,52),(2,52),(1,556),(1,556),(2,556),......
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
