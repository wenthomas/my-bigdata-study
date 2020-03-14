package com.wenthomas.sparkcore.keyvalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}

/**
 * @author Verno
 * @create 2020-03-14 13:48 
 */
object MyCombineByKey extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //combineByKey[C]:聚合算子，相比reduceByKey多了个动态0值（根据碰到的每个key的第一个value来动态生成），
    // 并且按照分区内和分区间分开两种聚合逻辑
    /**
     * 注意：
     * C内需要手动添加类型，否则会报错
     */
    //需求1：求每个Key的和
    private val combineRDD: RDD[(String, Int)] = rdd.combineByKey(
        v => v,
        (c: Int, v) => c + v,
        (c1: Int, c2: Int) => c1 + c2
    )

    //需求2：求每个key在每个分区内的最大值，然后再求出这些最大值的和
    private val combineRDD1: RDD[(String, Int)] = rdd.combineByKey(
        v => v,
        (max: Int, v: Int) => max.max(v),
        (max1: Int, max2: Int) => max1 + max2
    )

    //需求3：求每个Key的平均值
    private val tmp: RDD[(String, (Int, Int))] = rdd.combineByKey(
        v => (v, 1),
        {
            case ((sum: Int, count: Int), v) => (sum + v, count + 1)
        },
        {
            case ((sum1: Int, count1: Int), (sum2: Int, count2: Int)) => (sum1 + sum2, count1 + count2)
        }
    )
    private val combineRDD2: RDD[(String, Double)] = tmp.mapValues({ case (sum, count) => sum.toDouble / count })

    //4，行动算子
    private val result = combineRDD.collect
    private val result1 = combineRDD1.collect
    private val result2 = combineRDD2.collect

    //(c,18),(a,5),(b,3)
    println(result.mkString(","))
    //(c,12),(a,3),(b,3)
    println(result1.mkString(","))
    //(c,6.0),(a,2.5),(b,3.0)
    println(result2.mkString(","))

    //5，关闭连接
    sc.stop()

}
