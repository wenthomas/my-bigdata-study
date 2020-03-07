package com.wenthomas.collections.highFunctions

/**
 * @author Verno
 * @create 2020-03-07 21:39 
 */
/**
 * 滑窗函数
 */
object MySlide extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)
    //.sliding(a,b),意为以每a个元素为一个整体，b作为步长向右移动，直至滑窗检索不到下一组为止
    //得出结果为一个可迭代列表集合
    private val iterator:Iterator[List[Int]] = list1.sliding(3, 2)
    iterator.foreach(println)
}
