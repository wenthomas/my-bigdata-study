package com.wenthomas.collections.highFunctions

/**
 * @author Verno
 * @create 2020-03-07 22:19 
 */
/**
 * reduce()聚合函数
 */
object MyReduce extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)
    private val value: Int = list1.reduce((x, y) => x + y)

    println(value)

}
