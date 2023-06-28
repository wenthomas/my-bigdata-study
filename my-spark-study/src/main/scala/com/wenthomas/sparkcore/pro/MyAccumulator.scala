package com.wenthomas.sparkcore.pro

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-17 14:59 
 */
/**
 * 注意：
 * 1.累加器的更新操作最好放在action中, Spark 可以保证每个 task 只执行一次. 如果放在 transformations 操作中则不能保证只更新一次.有可能会被重复执行.
 */
object MyAccumulator {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyAccumulator").setMaster("local[2]")
        val sc = new SparkContext(conf)

        val arr = List(1,2,3,4,5,6,7,8,9)
        val rdd= sc.makeRDD(arr)

        //使用系统内置累加器
        val acc: LongAccumulator = sc.longAccumulator

        rdd.foreach(x => {
            acc.add(x)
        })

        println(acc.value)

        sc.stop()
    }

}
