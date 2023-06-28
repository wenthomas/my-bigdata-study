package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 14:23 
 */
object MySample extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(1,2,3,4,5,6,7,8,9)
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    /**
     * sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong)：抽样，
     * 参数解释：
     * withReplacement：抽出的样品要不要放回集合进入下一轮抽样
     *
     * fraction：抽样评分值，每个元素被抽取到的概率，
     * 例如：fraction=0时，没有元素会被抽样出来，fraction=1时，所有元素都会被抽样出来
     *
     * seed：随机数种子
     */

    private val sampleRDD: RDD[Int] = rdd.sample(false, 1)



    //4，行动算子
    private val result = sampleRDD.collect

    //
    println(result.mkString(","))

    //5，关闭连接
    sc.stop()
}
