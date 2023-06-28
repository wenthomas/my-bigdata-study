package com.wenthomas.collections.highFunctions

/**
 * @author Verno
 * @create 2020-03-07 21:48 
 */
/**
 * 迭代器：Scala中集合都可转化为迭代器进行操作
 */
object MyIterator extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)
    private val iterator: Iterator[Int] = list1.iterator

    //遍历迭代器方法一：
    while (iterator.hasNext) {
        println(iterator.next())
    }
    println("-------------------------------------")
    private val iterator1: Iterator[Int] = list1.iterator
    //遍历迭代器方法二：
    for (elem <- iterator1) {
        println(elem)
    }

}
