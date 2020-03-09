package com.wenthomas.pattern.homework

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author Verno
 * @create 2020-03-10 0:47 
 */
/**
 * 编写一个函数，计算 List[Option[Int]] 中所有非None值之和。分别使用 match 和不适用 match 来计算
 */
object MySumInt extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)

    //List(Some(30), None, None, None, Some(10), Some(20))
    private val options: List[Option[Int]] = list1.map(x => if (x < 40) Some(x) else None)

    //方法一：match模式匹配实现
    def mySumInt1(list: List[Option[Int]]): Int = {
        list.foldLeft(0)((sum, x) => {
            x.isEmpty match {
                case true =>
                    sum
                case false =>
                    sum + x.get
            }})
    }

    println(mySumInt1(options))

    //方法二：一般方法实现
    def mySumInt2(list: List[Option[Int]]): Int = {
        list.foldLeft(0)((sum, x) => {
            if (x.isEmpty) sum else sum + x.get
        })
    }

    println(mySumInt2(options))
}
