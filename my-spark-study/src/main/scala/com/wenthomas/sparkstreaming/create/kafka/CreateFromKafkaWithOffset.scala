package com.wenthomas.sparkstreaming.create.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-23 16:19 
 */
object CreateFromKafkaWithOffset {

    val groupId = "my_streaming_consumer_group"

    //kafka配置参数
    val kafkaParams = Map[String, String](
        "bootstrap.servers" -> "mydata01:9092, mydata02:9092, mydata03:9092",
        "group.id" -> groupId
    )
    val topic = Set("demo2")

    //
    val cluster = new KafkaCluster(kafkaParams)


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("CreateFromKafkaWithOffset")
        val ssc = new StreamingContext(conf, Seconds(3))

        val sourceStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            kafkaParams,
            readOffsets(),
            //返回目标offset读到的数据
            (handler: MessageAndMetadata[String, String]) => handler.message()
        )

        sourceStream.flatMap(_.split("\\W+"))
                .map((_, 1))
                .reduceByKey(_ + _)
                .print(1000)


        //手动维护offset信息：否则每次都会从最开始消费
        saveOffsets(sourceStream)

        ssc.start()
        ssc.awaitTermination()
    }

    /**
     * 读取自己维护的offset信息
     * @return
     */
    def readOffsets() = {
        //保存最终结果
        var resultMap = Map[TopicAndPartition, Long]()

        //获取目标topic的所有分区：不存在返回Either左值，存在则返回右值
        val partitionsEither = cluster.getPartitions(topic)

        partitionsEither match {
            case Right(topicAndPartitionSet) =>
                //获取到分区信息和它的offset
                val topicAndPartitionToLongEither = cluster.getConsumerOffsets(groupId, topicAndPartitionSet)

                topicAndPartitionToLongEither match {
                    case Right(map) =>
                        //表示每个topic的每个分区都已经存储过偏移量，表示曾经消费也维护过
                        resultMap ++= map

                    case _ =>
                        //表示这个topic的这个分区是第一次消费
                        topicAndPartitionSet.foreach(topicAndPartition => {
                            resultMap += (topicAndPartition -> 0L)
                        })
                }
            case _ =>
                //表示不存在任何topic
        }
        resultMap
    }


    /**
     * 自定义提交保存offset方法
     * 注意：保存offset一定从kafka消费到的直接的那个DStream来保存
     */
    def saveOffsets(sourceStream: InputDStream[String]) = {
        //保存offset一定从kafka消费到的直接的那个DStream来保存
        //foreachRDD：每个批次执行一次传递流的函数
        sourceStream.foreachRDD(rdd => {
            var map = Map[TopicAndPartition, Long]()

            //如果这个rdd是直接来自于kafka，则可以直接强转为 HasOffsetRanges
            //这个类型包含了本次消费的offsets的信息
            val hasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

            //封装了每个分区的偏移量
            val ranges = hasOffsetRanges.offsetRanges

            //遍历更新每个分区的offset偏移量
            ranges.foreach(offsetRange => {
                val key = offsetRange.topicAndPartition()
                //untilOffset：不包含当前offset，下次消费时从该offset开始
                val value = offsetRange.untilOffset
                map += (key -> value)
            })

            //保存更新offset到KafkaCluster：默认维护在zk中
            cluster.setConsumerOffsets(groupId, map)
        })
    }
}
