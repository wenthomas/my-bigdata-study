package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 0:15 
 */
/**
 * 利用模式匹配，编写一个 swap(arr: Array[Int]) 函数，交换数组中前两个元素的位置
 */
object MySwap extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)

    def mySwapFirstAndSecond(arr: Array[Int]): Array[Int] = {
        val result = arr match {
            case Array(a, b, _*) => {
                arr(0) = b
                arr(1) = a
                arr
            }
        }
        result
    }

    println(mySwapFirstAndSecond(list1.toArray).mkString(","))
}
