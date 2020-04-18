package com.wenthomas.flink.api.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
/**
 * @author Verno
 * @create 2020-04-18 2:55 
 */
/**
 * Sink：将流数据输出到kafka中
 */
object KafkaSink {
    def main(args: Array[String]): Unit = {
        //0,创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //1,Source：从kafka读取数据
        val props = new Properties()
        props.setProperty("bootstrap.servers", "mydata01:9092")
        props.setProperty("group.id", "consumer-flink-demo-group")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("auto.offset.reset", "latest")
        val sourceStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor-demo", new SimpleStringSchema(), props))

        //2,Transform

        //3,Sink：将流数据输出到kafka中
        sourceStream.addSink(new FlinkKafkaProducer011[String]("mydata01:9092", "sensor-sink-demo", new SimpleStringSchema()))

        sourceStream.print("msg")

        env.execute("Kafka Sink Demo")
    }
}
