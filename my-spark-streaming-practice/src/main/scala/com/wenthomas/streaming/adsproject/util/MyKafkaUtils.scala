package com.wenthomas.streaming.adsproject.util

import java.lang

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * @author Verno
 * @create 2020-03-24 22:22 
 */
/**
 * <!--考虑学习的目的, 使用另外一个streaming和kafka继承版本-->
 * <dependency>
 *      <groupId>org.apache.spark</groupId>
 *      <artifactId>spark-streaming-kafka-0-10_2.11</artifactId>
 *      <version>2.1.1</version>
 * </dependency>
 */
object MyKafkaUtils {

    private val kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "mydata01:9092,mydata02:9092,mydata03:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        //用于标识这个消费者属于哪个消费团体
        "group.id" -> "my_ads_consumers",
        //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
        //可以使用这个配置，latest自动重置偏移量为最新的偏移量
        "auto.offset.reset" -> "latest",
        //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
        //如果是false，会需要手动维护kafka偏移量. 本次我们仍然自动维护偏移量
        "enable.auto.commit" -> (true: lang.Boolean)
    )


    /**
     * 创建DStream，返回接收到的输入数据
     *      LocationStrategies：根据给定的主题和集群地址创建consumer
     *      LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
     *      ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
     *      ConsumerStrategies.Subscribe：订阅一系列主题
     *
     * @param ssc
     * @param topics
     * @return
     */
    def getKafkaStream(ssc: StreamingContext, topics: String*) = {
        KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics.toIterable, kafkaParams)
        ).map(_.value())
    }

}
