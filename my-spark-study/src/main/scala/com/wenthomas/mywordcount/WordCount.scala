package com.wenthomas.mywordcount

/**
 * 方法一：
 *          1，按照Key对数据进行聚合 groupByKey
 *          2,将value转换为数组，利用scala的sortBy或者sortWith进行排序（maxpValues）
 *          （缺点是数据量太大的话会OOM）
 * 方法二：
 *          （1）取出所有的key
 *          （2）对key进行迭代，每次取出一个key利用spark的排序算子进行排序
 * 方法三：
 *          （1）自定义分区器，按照key进行分区，使不同的key进到不同的分区
 *          （2）对每个分区运用spark的排序算子进行排序
 */
object WordCount {
    def main(args: Array[String]): Unit = {

    }

}
