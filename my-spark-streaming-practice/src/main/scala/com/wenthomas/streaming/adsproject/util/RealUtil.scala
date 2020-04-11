package com.wenthomas.streaming.adsproject.util

import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
 * @author Verno
 * @create 2020-03-24 23:19 
 */
object RealUtil {

    /**
     * 通过隐式类提供stream类型实现保存redis的隐式转换方法
     * @param stream
     */
    implicit class MyRedis(stream:DStream[((String, String), List[(String, Int)])]) {
        def saveToRedis = {
            //rdd写入外部设备需要使用foreachRDD算子
            stream.foreachRDD(rdd => {
                rdd.foreachPartition((it: Iterator[((String, String), List[(String, Int)])]) => {
                    //1，建立到redis的连接
                    val client = RedisUtils.getClient
                    //2，写数据到redis
                    it.foreach({
                        // ((2020-03-24,华中),List((3,14), (1,12), (2,8)))
                        case ((day, area), adsCountList) =>
                            val key = "area:ads:count" + day
                            val field = area
                            //把集合转换成json字符串：不能使用fastjson(不支持scala)，使用json4s
                            import org.json4s.JsonDSL._
                            val value = JsonMethods.compact(JsonMethods.render(adsCountList))
                            client.hset(key, field, value)
                    })
                    //3，关闭到redis的连接
                    client.close()
                })
            })
        }
    }

}
