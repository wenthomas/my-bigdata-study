package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 13:55 
 */
object MyFlatMap extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1 to 3, 1 to 5, 10 to 20)
    private val rdd= sc.makeRDD(arr)

    //3，转换
    //flatMap()：map + flattern，一定要保证返回值是一个集合，可完成一进多出、一进不出等需求
    private val flatMapRDD: RDD[Int] = rdd.flatMap(x => x)

    //4，行动算子
    private val result: Array[Int] = flatMapRDD.collect

    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
