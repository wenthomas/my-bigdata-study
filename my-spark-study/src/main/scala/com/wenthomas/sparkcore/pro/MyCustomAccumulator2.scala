package com.wenthomas.sparkcore.pro

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-17 15:28 
 */
object MyCustomAccumulator2 {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyCustomAccumulator2").setMaster("local[2]")
        val sc = new SparkContext(conf)

        val arr = List(1,2,3,4,5,6,7,8,9)
        val rdd= sc.makeRDD(arr)

        /**
         * 使用自定义累加器时需要先注册
         */
        val acc = new MyAcc
        sc.register(acc, "myacc")

        rdd.foreach(x => acc.add(x))

        println(acc.value)

        sc.stop()
    }

}

/**
 * 自定义累加器：
 * 需求：累加器value包含（sum, count, avg）
 * 目标：Map((sum, 10), (count, 5), (avg, 2))
 */
class MyAcc extends AccumulatorV2[Double, mutable.HashMap[String, Any]] {

    private var map = mutable.HashMap[String, Any]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[Double, mutable.HashMap[String, Any]] = {
        val acc = new MyAcc
        acc.map = map
        acc
    }

    override def reset(): Unit = map.clear()

    override def add(v: Double): Unit = {
        //对sum和count进行累加，在最后求avg
        map += "sum" -> (map.getOrElse("sum", 0D).asInstanceOf[Double] + v)
        map += "count" -> (map.getOrElse("count", 0L).asInstanceOf[Long] + 1)
    }

    override def merge(other: AccumulatorV2[Double, mutable.HashMap[String, Any]]): Unit = {
        //合并两个Map
        other match {
            case o: MyAcc =>
                map += "sum" -> (map.getOrElse("sum", 0D).asInstanceOf[Double] + o.map.getOrElse("sum", 0D).asInstanceOf[Double])
                map += "count" -> (map.getOrElse("count", 0L).asInstanceOf[Long] + o.map.getOrElse("count", 0L).asInstanceOf[Long])
            case _ =>
                throw new UnsupportedOperationException
        }
    }

    override def value: mutable.HashMap[String, Any] = {
        map += "avg" -> (map.getOrElse("sum", 0D).asInstanceOf[Double] / map.getOrElse("count", 0L).asInstanceOf[Long])
        map
    }
}