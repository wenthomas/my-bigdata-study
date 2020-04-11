package com.wenthomas.sparkcore.keyvaluerdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 15:02 
 */
object MySortByKey extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e"))
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //sortByKey:排序，针对key进行排序
    /**
     * 注意：
     * （1）只能用来排序k-v类型RDD
     * （2）sortBy功能更强大，用的更多
     * （3）即使有再大量的数据，也不会出现OOM（shuffle借用磁盘），所以，排序的时候尽量使用spark的排序，避免使用scala的排序
    */
    private val sortByKeyRDD: RDD[(Int, String)] = rdd.sortByKey()

    //4，行动算子
    private val result = sortByKeyRDD.collect

    //(1,a),(4,d),(10,b),(10,e),(11,c),(20,d)
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
