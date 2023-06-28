package com.wenthomas.sparkstreaming.out

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-24 11:29 
 */

/**
 * DStream 输出
 *
 * 注意：
 *  1.连接不能写在driver层面（序列化）；
 *  2.如果写在foreach则每个RDD中的每一条数据都创建，得不偿失；
 *  3.增加foreachPartition，在分区创建（获取）。
 */
object MySaveToMysql {

    private val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("MySaveAsTextFiles")
        val ssc = new StreamingContext(conf, Seconds(3))

        //将数据流保存在mysql中
        ssc.socketTextStream("mydata01", 9999)
                .flatMap(_.split("\\W+"))
                .map((_, 1))
                .reduceByKey(_ + _)
                .foreachRDD(rdd => {
                    //把rdd转成df
                    //1，先创建sparkSession
                    val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

                    //2，转换
                    import spark.implicits._
                    val df = rdd.toDF("word", "count")

                    //3，写入
                    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark", "streaming_out", props)
                })


        ssc.start()
        ssc.awaitTermination()
    }

}
