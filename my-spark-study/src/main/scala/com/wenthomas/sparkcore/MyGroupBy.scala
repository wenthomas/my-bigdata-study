package com.wenthomas.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-13 14:14 
 */
object MyGroupBy extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,2,3,4,5,6,7,8,9)
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //groupBy():分组， 每个分组内的元素顺序可能会与原顺序不同，因为发生了shuffle操作
    private val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(x => x % 3)


    //4，行动算子
    private val result = groupByRDD.collect

    //(0,CompactBuffer(3, 6, 9)),(1,CompactBuffer(1, 4, 7)),(2,CompactBuffer(2, 5, 8))
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
