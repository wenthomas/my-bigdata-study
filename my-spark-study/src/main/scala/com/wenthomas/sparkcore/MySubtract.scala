package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 16:48 
 */
object MySubtract extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,1,2,3,4,5,6,7,8,9)
    val arr1 = Array(43,52,556,23,1,3)
    private val rdd= sc.makeRDD(arr,3)
    private val rdd1: RDD[Int] = sc.makeRDD(arr1)

    //3，转换
    //subtract():求差集
    //注意：求差集时会去重后求差集，可能会发生shuffle打乱重组
    private val subtractRDD: RDD[Int] = rdd.subtract(rdd1)

    //4，行动算子
    private val result = subtractRDD.collect

    // 结果1,3
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
