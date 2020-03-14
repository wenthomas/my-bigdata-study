package com.wenthomas.sparkcore.keyvalue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-14 9:17 
 */
object MyPartitionBy extends App {

    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array((1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1))
    private val rdd= sc.makeRDD(arr,3)

    //3，转换
    //partitionBy:使用分区器对k-vRDD元素进行重新分区分步
    /**
     * 注意：
     * （1）RDD必须为k-v类型
     * （2）Spark默认提供HashPartitioner分区器
     * （3）使用自定义分区器时：需要继承Partitioner并重写numPartitions和getPartition方法
     * （4）不同的分区器可能导致数据倾斜
     */
    private val partitionByRDD: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(2))

    private val glomRDD: RDD[Array[(Int, Int)]] = partitionByRDD.glom()

    //4，行动算子
    private val result = glomRDD.collect

    //List((2,1), (4,1), (6,1), (8,1)), List((1,1), (3,1), (5,1), (7,1), (9,1))
    println(result.map(x => x.toList).mkString(","))

    //5，关闭连接
    sc.stop()

}
