package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 15:18 
 */
object MyCoalesce extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,2,3,4,5,6,7,8,9)
    private val rdd= sc.makeRDD(arr,5)

    //3，转换
    //coalesce():合并，一般用来减少分区
    //第二个参数为是否启用shuffle
    //注意：
    // (1)减少分区时，默认不会进行shuffle，是否shuffle取决于分区数据会不会分散到不同的分区去
    // (2)增加分区时，会造成shuffle
    //开发指导：当RDD数据倾斜时，可使用coalesce，并且第二个参数设为true人为造成shuffle打乱重组，重组合并后可缓解数据倾斜
    private val indexRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, list) => list.map(x => (index, x)))

    private val coalesceRDD: RDD[(Int, Int)] = indexRDD.coalesce(2)

    //结果2
    println(coalesceRDD.getNumPartitions)



    //4，行动算子
    private val result = indexRDD.collect
    private val result1 = coalesceRDD.collect

    //(0,1),(1,2),(1,3),(2,4),(2,5),(3,6),(3,7),(4,8),(4,9)
    println(result.mkString(","))
    println(result1.mkString(","))

    //5，关闭连接
    sc.stop()

}
