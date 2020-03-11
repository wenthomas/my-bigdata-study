package com.wenthomas.collections.highFunctions

/**
 * @author Verno
 * @create 2020-03-07 21:55 
 */
/**
 * 拉链函数
 */
object MyZip extends App {

    val list1 = List(30, 50, 70, 60, 10)
    val list2 = List(3, 5, 7, 6, 1, 2)

    //以list1为左表，list2为右表拉链，多出的部分去除
    private val results1: List[(Int, Int)] = list1.zip(list2)
    println(results1)
    println("-----------------------------------")

    //以list1为左表，list2为右表拉链，多出的部分分别补充为1和0
    private val results2: List[(Int, Int)] = list1.zipAll(list2, 1, 2)
    println(results2)
    println("-----------------------------------")

    //和自己的索引进行zip
    private val withIndex: List[(Int, Int)] = list1.zipWithIndex
    println(withIndex)
    println("-----------------------------------")

    //对Map分别把key和value取出组成key的集合和value的集合
    //要求：list中存储的是二维的元组的时候, 才能使用unzip
    val list3 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    private val result3: Any = list3.unzip
    println(result3)
}
