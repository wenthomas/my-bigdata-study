package com.wenthomas.collections.highFunctions

/**
 * @author Verno
 * @create 2020-03-07 22:16 
 */

/**
 * groupBy()分组函数
 */
object MyGroupBy extends App {

    val list1 = List("hello", "hello", "world", "atguigu", "hello", "world")

    private val wordMap: Map[String, List[String]] = list1.groupBy(x => x)
    println(wordMap)

}
