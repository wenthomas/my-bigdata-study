package com.wenthomas.sparkcore.keyvalue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 10:18 
 */
object MyGroupByKey extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(("Hello",1),("World",1),("Hello",1),("Scala",1),("Hello",1),("Spark",1),("World",1))
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //groupByKey(): 分组，k-v RDD元素按照key进行分组
    /**
     * 注意：
     * （1）RDD必须为k-v类型
     * （2）Spark默认提供HashPartitioner分区器
     */
    //默认按照Key的hash值进行分组
    private val groupByRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    //4，行动算子
    private val result = groupByRDD.collect

    // (Spark,CompactBuffer(1)),(World,CompactBuffer(1, 1)),(Scala,CompactBuffer(1)),(Hello,CompactBuffer(1, 1, 1))
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()

}
