package com.wenthomas.sparkcore.pro

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-17 16:20 
 */
/**
 * 广播变量：
 *
 * 1，使用：
 *      val broadcastVal = sc.broadcast(val) ----创建广播变量
 *      broadcastVal.value                   ----获取广播变量
 *
 * 2，正常情况下：
 *      闭包所用到的变量是以task为单位从driver传输到executor的，一个task对应一份变量副本。当数据量大时会占用大量资源并且冗余。
 *   使用广播变量后：
 *      包所用到的变量是以executor为单位从driver传输到executor，
 *      一个executor以下多个task（executor多内核的场景）可以共享这一份变量，可以有效减小数据传输的总量。
 */
object MyBroadcast {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyBroadcast").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val bigArr = 1 to 2000 toArray

        //创建广播变量
        val bigVal = sc.broadcast(bigArr)

        val list = List(123, 3243, 4543, 1500, 1000, 5000)

        val rdd = sc.makeRDD(list)

        //筛选大于2000的数据
        rdd.filter(x => !bigVal.value.contains(x)).foreach(println)


        sc.stop()
    }

}
