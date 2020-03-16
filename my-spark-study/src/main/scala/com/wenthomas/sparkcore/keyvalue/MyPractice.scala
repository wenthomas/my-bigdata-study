package com.wenthomas.sparkcore.keyvalue

import com.wenthomas.sparkcore.keyvalue.MyAggregateByKey.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-14 16:03 
 */
object MyPractice extends App {

    /**
     * 1.数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
     *              1516609143867 6 7 64 16
     *              1516609143869 9 4 75 18
     *              1516609143869 1 7 87 12
     * 需求: 统计出每一个省份广告被点击次数的 TOP3
     */

    /**
     * 倒推法：
     * RDD[(省份, List((广告1, 点击数), (广告2, 点击数), (广告3, 点击数)))]
     * <= RDD[(省份, List((广告1, 点击数), (广告2, 点击数), (广告3, 点击数), (广告4, 点击数), ...))] 做map，只对内部的list排序，取前3
     * <= RDD[(省份, (广告1, 点击数))] groupByKey
     * <= RDD[((省份, 广告1), 点击数)] map格式转换
     * <= RDD[(省份, 广告1), 1), (省份, 广告1), 1), (省份, 广告1), 1), ...] reduceByKey
     * <= RDD[(省份, 城市, 用户， 广告1), 1)] map字段截取
     */

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    val path: String = "file://E:/1015/1015课堂资料/502_大数据之 spark/02_资料/agent.log"
    private val lineRDD: RDD[String] = sc.textFile(path)

    //测试数据
//    private val lineRDD: RDD[String] = sc.makeRDD(List("1516609143867 6 7 64 16", "1516609143869 9 4 75 18"))

    private val mapRDD: RDD[((String, String), Int)] = lineRDD.map(x => {
        val arr = x.split(" ")
        ((arr(1), arr(4)), 1)
    })

    private val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    private val mapRDD2: RDD[(String, (String, Int))] = reduceRDD.map {
        case ((province, adv), count) => (province, (adv, count))
    }

    private val rankRDD: RDD[(String, List[(String, Int)])] = mapRDD2.groupByKey()
            .mapValues(list => list.toList.sortBy(x => x._2)(Ordering.Int.reverse).take(3))

    rankRDD.sortBy(x => x._1, ascending = true).collect.foreach(println)



}
 
