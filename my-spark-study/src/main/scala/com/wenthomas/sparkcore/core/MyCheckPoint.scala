package com.wenthomas.sparkcore.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-16 22:51 
 */

/**
 * 设置RDD checkpoint:
 * 1, sc设置checkpoint保存目录
 * 2， 在需要的地方设置检查点，一般是每个阶段结束前设置，可以减少重复计算
 * 3, 一般checkpoint和cache一起配合使用
 */
object MyCheckPoint extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[2]")
    private val sc = new SparkContext(conf)

    // 设置checkpoint存储目录，一般生产环境放在hdfs上
    sc.setCheckpointDir("ckeckpoint")


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
     * 设置RDD checkpoint
     */
    rdd2.checkpoint()
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
