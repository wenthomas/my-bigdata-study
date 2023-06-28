package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 15:36 
 */
object MyRepartition extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,2,3,4,5,6,7,8,9)
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //repartition():增加分区
    //注意：一定会shuffle
    private val repartitionRDD: RDD[Int] = rdd.repartition(5)


    //4，行动算子
    private val result = repartitionRDD.collect

    println(result.mkString(","))
    // 结果5
    println(repartitionRDD.getNumPartitions)

    //5，关闭连接
    sc.stop()
}
