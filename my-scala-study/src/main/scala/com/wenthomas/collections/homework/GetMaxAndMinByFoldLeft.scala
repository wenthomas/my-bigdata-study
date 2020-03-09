package com.wenthomas.collections.homework

import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-07 23:17 
 */
/**
 * 使用 foldLeft 同时计算最大值和最小值(一次折叠完成)
 */
object GetMaxAndMinByFoldLeft extends App {
    private val list1 = List(30, 5, 7, 60, 1, 20)
    //方法一：
    private val map = mutable.Map(("max", list1(0)), ("min", list1(0)))

    private val result = list1.foldLeft(map)((map, i) => {
        if (i >= map("max")) map + ("max" -> i)
        else if (i < map("min")) map + ("min" -> i)
        else map
    })
    println(result)

    println("-----------------------------------------------")
    //方法二：
    private val result1: Any = list1.foldLeft((list1.head, list1.head))((tup, e) => (tup._1.max(e), tup._2.min(e)))
    println(result1)

}
