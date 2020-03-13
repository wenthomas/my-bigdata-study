package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 15:02 
 */
case class User(name: String, age: Int)

object MyDistinct extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,2,3,4,5,6,7,8,9,1,1,2)
    private val rdd= sc.makeRDD(arr,3)
    private val rdd1: RDD[Product] = sc.makeRDD(Array(User("wenthomas", 10), ("Verno", 20), User("wenthomas", 12), User("wenthomas", 10)))

    //3，转换
    //distinct()：去重，去完重后顺序可能会发生变化
    private val distinctRDD: RDD[Int] = rdd.distinct()

    //比较对象时，因为样例类User自动实现了equals和hashcode方法，所以属性相同则对象相同
    //注意：如需比较特定属性，需要重写样例类的equals和hashcode方法
    private val distinctRDD1: RDD[Product] = rdd1.distinct()

    //将去重结果聚合为n个分区
    private val distinctRDD2: RDD[Product] = rdd1.distinct(2)


    //4，行动算子
    private val result = distinctRDD.collect
    private val result1 = distinctRDD1.collect
    private val result2 = distinctRDD2.collect

    // 6,3,9,4,1,7,8,5,2
    println(result.mkString(","))

    //User(wenthomas,12),User(wenthomas,10),(Verno,20)
    println(result1.mkString(","))
    println(result2.mkString(","))

    //5，关闭连接
    sc.stop()

}
