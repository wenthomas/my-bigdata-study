package com.wenthomas.sparkcore.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-16 21:33 
 */
object MyCache extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[2]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(10,20,30,40,50,60)
    private val rdd: RDD[Int] = sc.makeRDD(arr)

    private val rdd1: RDD[Int] = rdd.map(x => {
        println("map:" + x)
        x
    })

    private val rdd2: RDD[Int] = rdd1.filter(x => {
        println("filter:" + x)
        true
    })

    /**
     * 设置RDD持久化，默认存储级别：StorageLevel.MEMORY_ONLY
     */
    rdd2.cache()


    // 以下3个job只会看到一次map和filter的输出
    rdd2.collect
    println("分界线--------------------------------------------")
    rdd2.collect
    println("分界线--------------------------------------------")
    rdd2.collect
    println("分界线--------------------------------------------")





    // 关闭连接
    sc.stop()
}
