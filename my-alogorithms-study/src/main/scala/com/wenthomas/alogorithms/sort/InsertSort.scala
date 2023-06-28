package com.wenthomas.alogorithms.sort

import com.wenthomas.alogorithms.sort.BubbleSort.sort

import scala.util.control.Breaks
import scala.util.control.Breaks._

/**
 * @author Verno
 * @create 2020-04-11 14:11 
 */
/**
 * 插入排序
 */
object InsertSort {
    def main(args: Array[String]): Unit = {
        val arr = Array(30, 20, 40, 10, 60, 80, 50, 70)
        println(arr.mkString(","))
        val arr1 = sort(arr)
        println(arr.mkString(","))
    }

    def sort(arr: Array[Int]) = {
        for (i <- 0 until arr.length - 1) {
            Breaks.breakable {
                for (j <- i + 1 until (0, -1)) {
                    if (arr(j) < arr(j - 1)) {
                        val tmp = arr(j)
                        arr(j) = arr(j - 1)
                        arr(j - 1) = tmp
                    } else break
                }
            }
        }
    }
}

