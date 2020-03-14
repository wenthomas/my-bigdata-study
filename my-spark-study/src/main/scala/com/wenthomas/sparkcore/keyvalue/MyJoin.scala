package com.wenthomas.sparkcore.keyvalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 15:21 
 */
object MyJoin extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array((1, 10),(2, 20),(1, 100),(3, 30),(4,10))
    val arr1 = Array((1, "a"),(2, "b"),(1, "aa"),(3, "c"),(5,"ee"))
    private val rdd= sc.makeRDD(arr,3)
    private val rdd1= sc.makeRDD(arr1,3)

    //3，转换
    //join: 连接，含义和sql的join差不多，用来连接两个RDD
    /**
     * join(otherDataset, [numTasks])
     * 内连接:
     *      在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素对在一起的(K,(V,W))的RDD
     * 注意：
     * Spark默认提供HashPartitioner分区器
     * 只能用于k-v形式的RDD
     */
    private val joinRDD: RDD[(Int, (Int, String))] = rdd.join(rdd1)


    /**
     * leftOuterJoin：左外连接
     * 右边可能没有，所以使用Option类型
     */
    private val joinRDD1: RDD[(Int, (Int, Option[String]))] = rdd.leftOuterJoin(rdd1)


    /**
     * rightOuterJoin：右外连接
     * 左边可能没有，所以使用Option类型
     */
    private val joinRDD2: RDD[(Int, (Option[Int], String))] = rdd.rightOuterJoin(rdd1)


    /**
     * fullOuterJoin：全外连接
     * 左边右边都可能没有，所以使用Option类型
     */
    private val joinRDD3: RDD[(Int, (Option[Int], Option[String]))] = rdd.fullOuterJoin(rdd1)



    //4，行动算子
    private val result = joinRDD.collect
    private val result1 = joinRDD1.collect
    private val result2 = joinRDD2.collect
    private val result3 = joinRDD3.collect

    // 内连接：(3,(30,c)),(1,(10,a)),(1,(10,aa)),(1,(100,a)),(1,(100,aa)),(2,(20,b))
    println(result.mkString(","))
    // 左外连接：(3,(30,Some(c))),(4,(10,None)),(1,(10,Some(a))),(1,(10,Some(aa))),(1,(100,Some(a))),(1,(100,Some(aa))),(2,(20,Some(b)))
    println(result1.mkString(","))
    // 右外连接：(3,(Some(30),c)),(1,(Some(10),a)),(1,(Some(10),aa)),(1,(Some(100),a)),(1,(Some(100),aa)),(5,(None,ee)),(2,(Some(20),b))
    println(result2.mkString(","))
    // 全外连接：(3,(Some(30),Some(c))),(4,(Some(10),None)),(1,(Some(10),Some(a))),(1,(Some(10),Some(aa))),(1,(Some(100),Some(a))),
    // (1,(Some(100),Some(aa))),(5,(None,Some(ee))),(2,(Some(20),Some(b)))
    println(result3.mkString(","))

    //5，关闭连接
    sc.stop()

}
