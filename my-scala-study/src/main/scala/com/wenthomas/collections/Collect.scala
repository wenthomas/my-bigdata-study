package com.wenthomas.collections

import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-06 23:41 
 */
object Collect extends App {
    private val ints: Array[Int] = Array[Int](1,2,3,4)
    private val inclusive: Range.Inclusive = 1 to 10
    //不可变数组指定长数组，改变长度即新建新数组
    val arr1:Array[Int] = ints :+ 5
    println(arr1.mkString(","))
    private val ints1 = new Array[Int](5)
    private val ints2: Array[Int] = Array[Int](5)
    println(ints1.length)
    println(ints2.length)

    //队列
    private val queue: mutable.Queue[Int] = mutable.Queue[Int](10,20,30)
    queue.enqueue(100,200)
    queue.dequeue()
    println(queue)
}
