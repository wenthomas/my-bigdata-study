package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 10:38 
 */
object MyMap extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(10,20,30,40,50,60)
    private val rdd: RDD[Int] = sc.makeRDD(arr, 4)


    //3，转换
    //（1）map():每次处理一条数据。
    private val mapRDD: RDD[Int] = rdd.map(x => x * 2)

    //（2）mapPartitions():每次处理一个分区的数据（以分区作为整体），这个分区的数据处理完后，原 RDD 中该分区的数据才能释放，可能导致 OOM
    private val mapRDD1: RDD[Int] = rdd.mapPartitions(list => list.map(x => x * 2))

    //（3）mapPartitionsWithIndex():和mapPartitions(func)类似. 但是会给func多提供一个Int值来表示分区的索引.
    // 所以func的类型是:(Int, Iterator<T>) => Iterator<U>
    private val mapRDD2: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((numPartition, list) => list.map(x => (numPartition, x)))


    //4，行动算子
    private val result: Array[Int] = mapRDD.collect
    private val result1: Array[Int] = mapRDD1.collect
    private val result2: Array[(Int, Int)] = mapRDD2.collect


    println(result.mkString(","))
    println(result1.mkString(","))
    println(result2.mkString(","))

    //5，关闭连接
    sc.stop()

}
