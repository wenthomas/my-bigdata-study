package com.wenthomas.sparkstreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Verno
 * @create 2020-03-24 9:54 
 */
/**
 * 带状态转换算子：updateStateByKey
 *
 * 允许在使用新信息不断更新状态的同时能够保留他的状态.
 * 需要做两件事情:
 *      1.定义状态. 状态可以是任意数据类型
 *      2.定义状态更新函数. 指定一个函数, 这个函数负责使用以前的状态和新值来更新状态.
 */
object MyUpstateByKey {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("MyUpstateByKey")
        val ssc = new StreamingContext(conf, Seconds(3))

        ssc.checkpoint("file:///E:/1015/my_data/ckpoint1")

        ssc.socketTextStream("mydata01", 9999)
                        .flatMap(_.split("\\W+"))
                        .map((_, 1))
                        //updateStateByKey中泛型要加上，否则无法自动推导类型
                        .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                            Some(seq.sum + opt.getOrElse(0))
                        })
                        .print(1000)



        ssc.start()
        ssc.awaitTermination()

        ssc.stop(false)
    }

}
