package com.wenthomas.collections.homework

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

            it.foldLeft(stack)((stack, i) => stack.push(i)).toList
        }
    }
}
