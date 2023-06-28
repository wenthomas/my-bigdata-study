package com.wenthomas.sparkstreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-24 11:20 
 */

/**
 * 窗口操作 window：
 *
 * 窗口的两个要素：
 * 1，窗口的长度（设置时必须为周期的整数倍）
 * 2，窗口的滑动步长（默认为一个周期长度，且设置时必须为周期的整数倍）
 *
 * window(windowLength, slideInterval)
 */
object MyWindow {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("MyWindow")
        val ssc = new StreamingContext(conf, Seconds(3))

        //需求一：每6秒钟计算统计最近9秒内的wordCount
        ssc.socketTextStream("mydata01", 9999)
                //window对聚合算子起影响，其余无影响
                .window(Seconds(9), Seconds(6))
                .flatMap(_.split("\\W+"))
                .map((_, 1))
                .reduceByKey(_ + _)
                .print(1000)

        ssc.start()
        ssc.awaitTermination()
    }

}
