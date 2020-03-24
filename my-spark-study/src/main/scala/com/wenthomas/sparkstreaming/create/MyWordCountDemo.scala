package com.wenthomas.sparkstreaming.create

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Streaming 基本编程demo
 */
object MyWordCountDemo {

    def main(args: Array[String]): Unit = {
        //1, 创建StreamingContext
        //注意：setMaster("local[*]")：local至少为2，因为需要一个专门的executor用于数据接收器
        val conf = new SparkConf().setMaster("local[*]").setAppName("MyWordCountDemo")
        val ssc = new StreamingContext(conf, Seconds(3))

        //2，从数据源创建一个流：socket、rdd队列、自定义接收器、kafka（重点）
        val sourceStream = ssc.socketTextStream("mydata01", 9999)

        //3，对流做各种转换操作
        val result = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        //4，行动算子 print foreach foreachRDD
        result.print

        //5，启动流
        ssc.start()

        //6，阻止主线程退出
        ssc.awaitTermination()

    }

}
