package com.wenthomas.flink.api.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.util.Random
/**
 * Source读取数据流：
 * 1,从集合中读取数据
 * 2,从文件中读取
 * 3,从流中读取
 *      3.1,socket文本流中读取
 *      3.2,从kafka中读取
 * 4,从给定数据结构中读取
 * 5,从自定义Source中读取
 * @author Verno
 * @create 2020-04-17 11:45 
 */
object SourceDemo {
    def main(args: Array[String]): Unit = {
        //0,创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //--------------------------------------------------------------------------------------------------------------
        //1,从集合中读取数据
        val list = List(
            SensorReading("sensor_1", 1547718199, 35.8),
            SensorReading("sensor_6", 1547718201, 15.4),
            SensorReading("sensor_7", 1547718202, 6.7),
            SensorReading("sensor_8", 1547718202, 35.3),
            SensorReading("sensor_4", 1547718203, 32.8),
            SensorReading("sensor_2", 1547718254, 20.4),
            SensorReading("sensor_11", 1547718212, 29.0),
            SensorReading("sensor_5", 1547718255, 27.5),
            SensorReading("sensor_10", 1547718212, 38.1)
        )
        val streamFromCollection = env.fromCollection(list)

        //--------------------------------------------------------------------------------------------------------------
        //2,从文件中读取
        val streamFromText = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")

        //--------------------------------------------------------------------------------------------------------------
        //3,从流中读取
        //3.1,socket文本流中读取
        val streamFromSocket = env.socketTextStream("mydata01", 7777)

        //3.2,从kafka中读取
        val props = new Properties()
        props.setProperty("bootstrap.servers", "mydata01:9092")
        props.setProperty("group.id", "consumer-flink-demo-group")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("auto.offset.reset", "latest")
        val streamFromKafka = env.addSource(new FlinkKafkaConsumer010[String]("sensor-demo", new SimpleStringSchema(), props))



        //--------------------------------------------------------------------------------------------------------------
        //4,从给定数据结构中读取
        val streamFromElement = env.fromElements((10, "tom", "Shenzhen"))


        //--------------------------------------------------------------------------------------------------------------
        //5,从自定义Source中读取(一般用于测试)
        val streamFromCustomSource = env.addSource(new MySensorSource())

        streamFromCollection.print("streamFromCollection")
        streamFromText.print("streamFromText")
        streamFromKafka.print("streamFromKafka")
        streamFromElement.print("streamFromElement")
        streamFromCustomSource.print("streamFromCustomSource")

        env.execute("stream from Collection")
    }

}

case class SensorReading(id: String, timestamp: Long, temperature: Double) {

}

/**
 * 自定义Source
 */
class MySensorSource() extends SourceFunction[SensorReading] {

    var running = true

    /**
     * 运行读取数据
     *
     * @param ctx 上下文
     */
    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
        //定义一个随机数发生器
        val rand = new Random()

        //随机生成10个传感器的温度值，并且不停更新
        var curTemps = (1 to 10).map({
            i => ("sensor_" + i, 30 + rand.nextGaussian() * 20)
        })

        //循环生成数据流
        while (running) {
            //在当前温度基础上随机生成微小波动
            curTemps = curTemps.map({
                data => (data._1, data._2 + rand.nextGaussian())
            })

            //获取当前系统时间
            val curTime = System.currentTimeMillis()

            //封装入样例类（输出格式），然后用ctx上下文发出数据
            curTemps.foreach({
                data => ctx.collect(SensorReading(data._1, curTime, data._2))
            })

            Thread.sleep(1000L)
        }

    }

    /**
     * 停止读取数据
     */
    override def cancel(): Unit = {
        running = false
    }
}