package com.wenthomas.mywordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-12 0:15 
 */
object MyWordCount {
    def main(args: Array[String]): Unit = {
        // 1. 创建 SparkConf对象, 并设置 App名字
        // idea本地提交时设置.setMaster("local[*]")
//        val conf = new SparkConf().setAppName("MyWordCount").setMaster("local[*]")
        // 当提交yarn时，不能设置setMaster，在提交命令中再设置
            val conf = new SparkConf().setAppName("MyWordCount")
        // 2. 创建SparkContext对象
        val sc = new SparkContext(conf)

        // 3. 使用sc创建RDD并执行相应的transformation和action
        val lineRDD = sc.textFile(args(0))

        val resultRDD = lineRDD.flatMap(x => x.split("\\W+")).map(x => (x, 1)).reduceByKey((x, y) => x + y)

        // 4. 执行一个行动算子   (collect: 把各个节点计算后的数据, 拉取到驱动端)
        val wordCountArr = resultRDD.collect
        wordCountArr.foreach(println)

        // 5. 关闭连接
        sc.stop()
    }
}
