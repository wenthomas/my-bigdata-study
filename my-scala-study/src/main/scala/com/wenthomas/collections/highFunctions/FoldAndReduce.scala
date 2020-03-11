package com.wenthomas.collections.highFunctions

/**
 * 折叠函数foldLeft():
 * 与reduce()聚合的区别在于foldLeft函数可以加入初始值进行折叠，这个初始值可以为普通数据类型以及集合等等
 */
object FoldAndReduce extends App {
    val list1 = List(30, 50, 70, 60, 10, 20)

    //左折叠
    //另一种写法：val result = (0 /: list1)(_ + _)
    private val result = list1.foldLeft("HEAD:")(_ + _)
    println(result)
    println("------------------------------------------------------")


    //右折叠
    //另一种写法：val result = (list1 :\ 0) (_ + _)
    private val result2: String = list1.foldRight(":TAIL")(_ + _)
    println(result2)
}
