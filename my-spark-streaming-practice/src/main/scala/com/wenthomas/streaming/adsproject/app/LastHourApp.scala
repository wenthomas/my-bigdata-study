package com.wenthomas.streaming.adsproject.app
import com.wenthomas.streaming.adsproject.bean.AdsInfo
import com.wenthomas.streaming.adsproject.util.RedisUtils
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
 * @author Verno
 * @create 2020-03-25 10:58 
 */

/**
 * 最近 1 小时广告点击量实时统计
 */
object LastHourApp extends MyApp {
    override def dosomething(adsInfoStream: DStream[AdsInfo]): Unit = {
        adsInfoStream
                //1，先把窗口分好
                .window(Minutes(60), Seconds(3))
                //2，按照（广告，分钟）进行聚合
                .map(info => (info.adsId, info.hmString) -> 1)
                .reduceByKey(_ + _)
                //3，再按照广告分组，把这个广告下所有的分钟记录放在一起
                .map({
                    case ((ads, hm), count) => (ads, (hm, count))
                })
                .groupByKey()
                //4，写入到Redis中
                .foreachRDD(rdd => {
                    rdd.foreachPartition((it: Iterator[(String, Iterable[(String, Int)])]) => {
                        //只是判断是否有下一个元素，指针不会跳过这个元素
                        //注意：!iterator.isEmpty 等价于iterator.nonEmpty, 都只用来判断是否有下一个元素，不能通过.size来判断（会调用该迭代器元素，下次再调用就是空了）
                        if (it.nonEmpty) {
                            //1，建立到redis的连接
                            val client = RedisUtils.getClient
                            //2，写元素到redis
                            import org.json4s.JsonDSL._

                            val map = it.toMap.map({
                                case (adsId, it) => (adsId, JsonMethods.compact(JsonMethods.render(it)))
                            })
                            import scala.collection.JavaConversions._
                            // scala集合转换成java集合
                            val key = "lsat:ads:hour:count"
                            println(map)
                            //批次写入
                            client.hmset(key, map)
                            //3，关闭redis连接
                            client.close()
                        }
                    })
                })
    }
}


/*
统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量, 每6秒统计一次
1. 各广告, 每分钟               ->            按照 (广告, 分钟) 分组
2. 最近1小时,每6秒统计一次  ->          窗口: 窗口长度1小时  窗口的滑动步长 5s

----

1. 先把窗口分好
2. 按照广告分钟 进行聚合
3. 再按照广告分组, 把这个广告下所有的分钟记录放在一起
4. 把结果写在redis中


-------

写到redis'的时候的数据的类型:
1.
    key                             value
    广告id                          json字符串每分钟的点击

2.
key                                          value
"last:ads:hour:count"                        hash
                                             field              value
                                             adsId              json字符串
                                             "1"                {"09:24": 100, "09:25": 110, ...}

 */