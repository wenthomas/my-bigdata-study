package com.wenthomas.collections.highFunctions

/**
 * @author Verno
 * @create 2020-03-07 22:21 
 */
/**
 * foldLeft()的增强版：得出的集合长度比原集合+1，加上了初始值
 * 结果的每个元素都是每个阶段聚合得出的结果
 * scanRight:从右往左执行
 */
object MyScanLeft extends App {
    val list1 = List(30, 50, 70, 60, 10, 20)

    private val value: List[String] = list1.scanLeft("HEAD:")(_ + _)
    println(value)
}
