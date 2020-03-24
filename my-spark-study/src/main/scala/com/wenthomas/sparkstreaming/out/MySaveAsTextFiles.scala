package com.wenthomas.sparkstreaming.out

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-24 11:25 
 */

/**
 * DStream 输出
 *
 * 注意：
 *  1.连接不能写在driver层面（序列化）；
 *  2.如果写在foreach则每个RDD中的每一条数据都创建，得不偿失；
 *  3.增加foreachPartition，在分区创建（获取）。
 */
object MySaveAsTextFiles {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("MySaveAsTextFiles")
        val ssc = new StreamingContext(conf, Seconds(3))

        ssc.socketTextStream("mydata01", 9999)
                .flatMap(_.split("\\W+"))
                .map((_, 1))
                .reduceByKey(_ + _)
                //输出为word-xxxxxxx.log，输出目录默认为当前项目
                .saveAsTextFiles("word", "log")

        ssc.start()
        ssc.awaitTermination()
    }

}
