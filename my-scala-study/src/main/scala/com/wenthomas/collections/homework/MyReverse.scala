package com.wenthomas.collections.homework

import java.util

import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-08 16:33 
 */
/**
 * 不使用 reverse, 使用 foldLeft 实现字符串的反转(或者集合的反转)
 */
object MyReverse extends App {
    val list1 = List(30, 50, 70, 60, 10, 20)

    println(list1.myReverse)

    implicit class RichCollections(it: List[Any]) {

        def myReverse():List[Any] = {
            val stack = mutable.Stack[Any]()
            //方法一：利用栈结构的原理实现
            it.foldLeft(stack)((stack, i) => stack.push(i)).toList

            //方法二：利用foldLeft
            //it.foldLeft(List[Any]())((x, y) => y :: x)

            //方法三：利用递归实现
/*            if (it.isEmpty) it
                //把空参改为传List[Any}
            else myReverse(it.tail) :+ it.head*/
        }
    }
}
