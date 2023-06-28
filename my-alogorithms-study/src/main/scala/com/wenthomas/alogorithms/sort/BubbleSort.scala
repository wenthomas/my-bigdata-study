package com.wenthomas.alogorithms.sort

/**
 * @author Verno
 * @create 2020-04-11 13:49 
 */
/**
 * 冒泡排序
 */
object BubbleSort {
    def main(args: Array[String]): Unit = {
        val arr = Array(30, 20, 40, 10, 60, 80, 50, 70)
        println(arr.mkString(","))
        val arr1 = sort(arr)
        println(arr.mkString(","))
    }

    def sort(arr: Array[Int]) = {
        for (j <- 0 until arr.length - 1) {
            for (i <- 0 until arr.length - 1 - j) {
                if (arr(i) > arr(i + 1)) {
                    val tmp = arr(i)
                    arr(i) = arr(i + 1)
                    arr(i + 1) = tmp
                }
            }
        }
        arr
    }
}
