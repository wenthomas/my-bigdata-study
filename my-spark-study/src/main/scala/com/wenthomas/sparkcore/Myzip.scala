package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 16:53 
 */
object Myzip extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,1,2,3,4,5,6,7,8,9)
    val arr1 = Array(43,52,556,23,1,3,12,234,54,23)
    val arr2 = Array(33,32,43,55)
    private val rdd= sc.makeRDD(arr,3)
    private val rdd1: RDD[Int] = sc.makeRDD(arr1, 3)
    private val rdd2: RDD[Int] = sc.makeRDD(arr2, 3)

    //3，转换
    //zip():拉链操作
    /**
     * 注意：spark的拉链与scala的拉链有区别，需要遵守以下条件
     * （1）每个对应分区的元素个数应该是相同的，否则会报错
     * （2）两个RDD的分区数也要保持一致
     * 总结：两个RDD的总元素数相等，分区数也要相等
     */
    private val zipRDD= rdd.zip(rdd1)




    //zipPartitions()：可以调用scala的zip，此时只需保证分区数相同即可
    private val zipRDD2: RDD[(Int, Int)] = rdd.zipPartitions(rdd2)((it1, it2) => {
        it1.zip(it2)
    })

    //zipWithIndex()：RDD添加索引号
    private val withIndex: RDD[(Int, Long)] = rdd.zipWithIndex

    //4，行动算子
    private val result = zipRDD.collect
    private val result1 = zipRDD2.collect
    private val result2 = withIndex.collect

    // zip结果(1,43),(1,52),(2,556),(3,23),(4,1),(5,3),(6,12),(7,234),(8,54),(9,23)
    println(result.mkString(","))
    // zipPartitions结果(1,33),(3,32),(6,43),(7,55)
    println(result1.mkString(","))
    // zipWithIndex结果(1,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)
    println(result2.mkString(","))

    //5，关闭连接
    sc.stop()

}
