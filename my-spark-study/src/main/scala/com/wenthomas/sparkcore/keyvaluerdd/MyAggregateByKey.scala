package com.wenthomas.sparkcore.keyvaluerdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 11:19 
 */
object MyAggregateByKey extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))
    private val rdd= sc.makeRDD(arr,2)

    //3，转换
    //aggregateByKey:聚合算子，相比reduceByKey多了个0值参数，并且按照分区内和分区间分开两种聚合逻辑
    /**
     * aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])
     * 注意：
     * (1)zeroValue： 0值
     * (2)seqOp ： 分区内聚合操作
     * (3)combOp ： 分区间聚合操作
     */

    //需求一：计算每个分区想用key的最大值，然后相加
    //private val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)((u, v) => u.max(v), (u1, u2) => u1 + u2)
    private val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)(_.max(_), _ + _)

    //需求二：同时计算出最大值的和和最小值的和
    private val aggregateByKeyRDD1: RDD[(String, (Int, Int))] = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))({
        case ((max, min), v) => (max.max(v), min.min(v))
    }, {
        case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
    })

    //需求三：计算每个key的平均值
    private val rdd2: RDD[(String, Double)] = rdd.aggregateByKey((0, 0))({
        case ((sum, count), v) => (sum + v, count + 1)
    }, {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
    }).map({
        case (key, (sum, count)) => (key, sum.toDouble / count)
    })

    //4，行动算子
    private val result = aggregateByKeyRDD.collect
    private val result1: Array[(String, (Int, Int))] = aggregateByKeyRDD1.collect
    private val result2 = rdd2.collect

    // (b,3),(a,3),(c,12)
    println(result.mkString(","))

    // (b,(3,3)),(a,(3,2)),(c,(12,10))
    println(result1.mkString(","))

    // (b,3.0),(a,2.5),(c,6.0)
    println(result2.mkString(","))

    //5，关闭连接
    sc.stop()

}
