package com.wenthomas.sparkcore.keyvalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 10:40 
 */
object MyFoldByKey extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = List(("female",1),("male",5),("female",5),("male",2))
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //foldByKey:聚合算子，相比reduceByKey多了个0值参数
    /**
     * 注意：
     * (1)相比reduceByKey多了个0值参数
     * (2)0值只在每个分区内会参与聚合运算，即只在预聚合阶段参与运算
     */
    private val foldByKeyRDD: RDD[(String, Int)] = rdd.foldByKey(0)((x, y) => x + y)


    //4，行动算子
    private val result = foldByKeyRDD.collect

    //(male,7),(female,6)
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()
}
