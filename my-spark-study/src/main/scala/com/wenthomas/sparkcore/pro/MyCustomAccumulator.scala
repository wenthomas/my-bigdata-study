package com.wenthomas.sparkcore.pro

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-17 15:13 
 */
/**
 * 自定义累加器：
 * 1，编写自定义累加器继承AccumulatorV2
 * 2，注册自定义累加器
 */
object MyCustomAccumulator {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyCustomAccumulator").setMaster("local[2]")
        val sc = new SparkContext(conf)

        val arr = List(1,2,3,4,5,6,7,8,9)
        val rdd= sc.makeRDD(arr)

        /**
         * 使用自定义累加器时需要先注册
         */
        val acc = new MyIntAcc
        sc.register(acc, "myacc")

        rdd.foreach(x => {
            acc.add(1)
        })

        println(acc.value)

        sc.stop()
    }
}

/**
 * 自定义Int累加器：继承AccumulatorV2
 */
class MyIntAcc extends AccumulatorV2[Int, Int] {

    private var sum = 0

    /**
     * 判"零"，对缓冲区进行判"零"
     * @return
     */
    override def isZero: Boolean = sum == 0

    /**
     * 复制累加器：把当前的累加器复制为一个新的累加器
     * @return
     */
    override def copy(): AccumulatorV2[Int, Int] = {
        val acc = new MyIntAcc
        acc.sum = sum
        acc
    }

    /**
     * 重置累加器：把缓冲区的值重置为零值zeroValue
     */
    override def reset(): Unit = sum = 0

    /**
     * 累加逻辑方法（分区内累加）
     * @param v
     */
    override def add(v: Int): Unit = sum += v

    /**
     *  分区间累加结果合并
     * @param other
     */
    override def merge(other: AccumulatorV2[Int, Int]): Unit = {
        other match {
            case acc: MyIntAcc =>
                this.sum += acc.sum
            case _ =>
                //或者抛异常
                this.sum += 0
        }
    }

    /**
     *  返回累加器运算后的最终值
     * @return
     */
    override def value: Int = sum
}