package com.wenthomas.flink.api.sink

import java.util

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
/**
 * @author Verno
 * @create 2020-04-18 9:34 
 */
/**
 * Sink：将数据流输出到ES
 */
object ESSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        //1,定义一个httpHosts
        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("mydata01", 9200))

        //2,定义一个ElasticSearchSinkFunction
        val esSinkFunction = new ElasticsearchSinkFunction[SensorReading] {
            override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                //(1)封装写入ES的数据
                val dataSource = new util.HashMap[String, String]()
                dataSource.put("sensor_id", t.id)
                dataSource.put("temperature", t.temperature.toString)
                dataSource.put("ts", t.timestamp.toString)
                //(2)创建一个index request
                val indexRequest = Requests.indexRequest()
                        .index("sensor_demo")
                        .`type`("readingData")
                        .source(dataSource)
                //(3)用requestIndexer发送http请求
                requestIndexer.add(indexRequest)
                println(t + "saved successfully!")
            }
        }

        //3,将数据流输出到ES中
        val esSink = new ElasticsearchSink.Builder[SensorReading](httpHosts, esSinkFunction)
        dataStream.addSink(esSink.build())


        env.execute()
    }

}
