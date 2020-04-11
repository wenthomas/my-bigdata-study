package com.wenthomas.sparkcore.keyvaluerdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 15:14 
 */
object MyCogroup extends App {



    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array((1, 10),(2, 20),(1, 100),(3, 30))
    val arr1 = Array((1, "a"),(2, "b"),(1, "aa"),(3, "c"))
    private val rdd= sc.makeRDD(arr,3)
    private val rdd1= sc.makeRDD(arr1,3)

    //3，转换
    //cogroup: 拼接，两个RDD按照key相同的元素，各个都拼接为一个组合，
    /**
     * cogroup(otherDataset, [numTasks])
     * 作用：在类型为(K,V)和(K,W)的 RDD 上调用，返回一个
     * 注意：
     * Spark默认提供HashPartitioner分区器
     */
    private val coGroupRDD: RDD[(Int, (Iterable[Int], Iterable[String]))] = rdd.cogroup(rdd1)

    //4，行动算子
    private val result = coGroupRDD.collect

    // (3,(CompactBuffer(30),CompactBuffer(c))),(1,(CompactBuffer(10, 100),CompactBuffer(a, aa))),(2,(CompactBuffer(20),CompactBuffer(b)))
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
