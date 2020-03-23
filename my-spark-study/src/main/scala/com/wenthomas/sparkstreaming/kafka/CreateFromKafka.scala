package com.wenthomas.sparkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-23 14:29 
 */

/**
 * 直连kafka数据源并设置checkponit
 * 注意：数据不会丢失也不会重复
 * 缺点：checkpoint慢慢累积会造成小文件过多，如果是保存在hdfs上会降低hdfs性能
 */
object CreateFromKafka {

    var checkpointPath = "file:///E:/1015/my_data/ckpoint"

    def main(args: Array[String]): Unit = {
        /**
         * 容错式创建ssc: 从checkpoint中恢复一个StreamingContext，
         * 如果checkpoint不存在，则调用后面的函数去创建一个ssc。
         * 注意：createSSCFunc不需要括号，表示传递一个函数交个StreamingContext，在不存在checkpoint时调用我们自定义的createSSCFunc函数
         */
        val ssc = StreamingContext.getActiveOrCreate(checkpointPath, createSSCFunc)


        ssc.start()
        ssc.awaitTermination()

    }

    def createSSCFunc() = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("CreateFromKafka")
        val ssc = new StreamingContext(conf, Seconds(3))

        //把offset的跟踪状态持久化在checkpoint中
        ssc.checkpoint(checkpointPath)

        //kafka配置参数
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> "mydata01:9092, mydata02:9092, mydata03:9092",
            "group.id" -> "my_streaming_consumer_group"
        )

        //使用直连方式创建DStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            kafkaParams,
            //设置topics：若集群中无此topic，会自动创建该topic，但第一次运行会报错（无此topic）
            Set("demo1")
        ).flatMap{
            case (_, v) =>
                v.split("\\W+")
        }.map((_, 1)).reduceByKey(_ + _).print

        ssc
    }

}
