package com.wenthomas.sparkcore.keyvaluerdd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 10:05 
 */
object MyReduceByKey extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = List(("female",1),("male",5),("female",5),("male",2))
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //reduceByKey:聚合算子，以key为组分别聚合value部分
    /**
     * 注意：
     * Spark默认提供HashPartitioner分区器
     */
    private val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => x + y)


    //4，行动算子
    private val result = reduceRDD.collect

    //(male,7),(female,6)
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()
}
