package com.wenthomas.sparkstreaming.create

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 从RDD队列中读取数据
 */
object CreateSSCFromRDDQueue {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("CreateSSCFromRDDQueue")
        val ssc = new StreamingContext(conf, Seconds(3))

        //将一段时间内的int数据封装为rdd队列
        val rdds = mutable.Queue[RDD[Int]]()
        //从rdd 队列中读取数据：一般用于压力测试
        val sourceStream = ssc.queueStream(rdds, false)

        val result = sourceStream.reduce(_ + _)
        result.print

        ssc.start()

        val sc = ssc.sparkContext
        while (true) {
            //循环向queue中插入rdd数据
            rdds.enqueue(sc.makeRDD(1 to 100))

            Thread.sleep(10)
        }

        ssc.awaitTermination()
    }

}
