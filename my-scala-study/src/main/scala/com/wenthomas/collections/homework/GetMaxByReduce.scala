package com.wenthomas.collections.homework

/**
 * @author Verno
 * @create 2020-03-07 23:14 
 */
/**
 * 使用 reduce 计算集合中的最大值.
 */
object GetMaxByReduce extends App {

    private val list1 = List(30, 5, 7, 60, 1, 20)

    //两两比较取最大值
    private val result: Any = list1.reduce((x, y) => if (x >= y) x else y)
    println(result)

}
