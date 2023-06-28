package com.wenthomas.sparkstreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-24 10:32 
 */
/**
 * 窗口操作 window：
 *
 * 窗口的两个要素：
 * 1，窗口的长度（设置时必须为周期的整数倍）
 * 2，窗口的滑动步长（默认为一个周期长度，且设置时必须为周期的整数倍）
 */
object MyReduceByKeyAndWindow {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("MyReduceByKeyAndWindow")
        val ssc = new StreamingContext(conf, Seconds(3))

        //需求一：每6秒钟计算统计最近9秒内的wordCount
        ssc.socketTextStream("mydata01", 9999)
                        .flatMap(_.split("\\W+"))
                        .map((_, 1))
//                        .reduceByKeyAndWindow(_ + _, Seconds(9))
                        // 窗口长度为9s，窗口步长为6s
                        //.reduceByKeyAndWindow(_ + _, Seconds(9), slideDuration = Seconds(6))
                        //优化：用旧的和新进的和走掉的，他们之间的数学关系进行优化，新窗口和旧窗口之间有重叠的才需要做此优化，
                        //      避免了部分重复计算
                        .reduceByKeyAndWindow(_ + _, _ - _,Seconds(9), slideDuration = Seconds(6))
                        .print(1000)

        //------------------------------------------------------------------------------------------------------------

        ssc.start()
        ssc.awaitTermination()
    }

}
