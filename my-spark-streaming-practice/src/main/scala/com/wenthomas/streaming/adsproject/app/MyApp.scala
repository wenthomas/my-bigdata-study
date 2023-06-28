package com.wenthomas.streaming.adsproject.app

import com.wenthomas.streaming.adsproject.bean.AdsInfo
import com.wenthomas.streaming.adsproject.util.MyKafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-24 22:35 
 */
/**
 * 共用app模板
 */
trait MyApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("MyApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        val topic = "my_ads_log"

        //设置checkpoint
        ssc.checkpoint("file:///E:/1015/my_data/my_ads_log_checkpoint")

        val sourceStream = MyKafkaUtils.getKafkaStream(ssc, topic)

        //将读到的kafka消息抽出封装为对象
        val adsInfoStream = sourceStream.map(s => {
            val split = s.split(",")
            AdsInfo(split(0).toLong, split(1), split(2), split(3), split(4))
        })

        //抽象方法
        dosomething(adsInfoStream)

        ssc.start()
        ssc.awaitTermination()
    }

    def dosomething(adsInfoStream: DStream[AdsInfo]): Unit

}
