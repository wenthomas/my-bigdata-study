package com.wenthomas.collections

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
}
