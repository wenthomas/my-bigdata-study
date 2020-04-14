package com.wenthomas.alogorithms.sort

import scala.util.Random

/**
 * @author Verno
 * @create 2020-04-11 14:27 
 */
/**
 * 快速排序：针对数据量较大的场景效果比较明显
 */
object QuickSort {
    def main(args: Array[String]): Unit = {
        var arr1 = Array(5,4,3,2,1)
        sort(arr1)
        println(arr1.mkString(","))

/*        val arr = randomArr()
        sort(arr)
        println(arr.mkString(","))*/

        //val arr1 = scalaQuickSort(arr)
        //println(arr1.mkString(","))

        //println(scalaQuickSortList(arr.toList).take(100).mkString(","))


        //val arr2 = Array(5,4,3,2,1)
        val arr2 = randomArr()
        val array = scalaQuickSortWithPartition(arr2)
        println(array.mkString(","))


    }

    def randomArr() = {
        val random = new Random()
        (1 to 2000).map(_ => random.nextInt(1000)).toArray
    }

    def swap(arr: Array[Int], a: Int, b: Int) = {
        val tmp = arr(a)
        arr(a) = arr(b)
        arr(b) = tmp
    }
    /**
     * 快速排序：递归实现，原地排序
     * @param arr
     */
    def sort(arr: Array[Int]) = {

        //分区：分为左区和右区
        def partition(array: Array[Int], left: Int, right: Int) = {
            //左右指针
            var low = left
            var high = right
            //基准值
            val zero = arr(left)

            while (low < high) {
                //注意：low <= high 与 arr(low)的顺序不能调换，否则某些情况会导致下角标越界异常，例如（5,4,3,2,1）序列
                while (low <= right && arr(low) <= zero) {
                    low += 1
                }

                while (left <= high && arr(high) > zero) {
                    high -= 1
                }

                if (low < high) {
                    swap(arr, low, high)
                }
            }

            //返回基准值的正确位置
            swap(arr, left, high)

            high
        }

        //排序，left开始的坐标，right结束的坐标  [left, right]
        def quick(arr: Array[Int], left: Int, right: Int): Unit = {
            //当左指针到了右指针右边，结束
            if (left >= right) return

            //基准值的位置
            val mid = partition(arr, left, right)
            //对左边进行排序
            quick(arr, left, mid - 1)
            //对右边进行排序
            quick(arr, mid + 1, right)
        }

        quick(arr, 0, arr.length - 1)
    }

    /**
     * 快速排序：返回新数组
     * @param arr
     * @return
     */
    def scalaQuickSort(arr: Array[Int]): Array[Int] = {
        arr match {
            case Array(p, rest@_*) =>
                //找到小于p的所有元素
                val left = scalaQuickSort(rest.filter(_ <= p).toArray)
                //找到大于p的所有元素
                val right = scalaQuickSort(rest.filter(_ > p).toArray)

                (left :+ p) ++ right
            case _ => Array()
        }
    }


    def scalaQuickSortList(list: List[Int]): List[Int] = {
        list match {
            case p :: rest =>
                scalaQuickSortList(rest.filter(_ <= p)) ::: p ::scalaQuickSortList(rest.filter(_ > p))
            case Nil => Nil
        }
    }

    /**
     * 快速排序：使用partition算子进行左右分区
     * @param arr
     * @return
     */
    def scalaQuickSortWithPartition(arr: Array[Int]): Array[Int] = {
        arr match {
            case Array(p, rest@_*) =>
                val (left, right)= rest.partition(_ <= p)
                (scalaQuickSortWithPartition(left.toArray) :+ p) ++ scalaQuickSortWithPartition(right.toArray)
            case _ => Array()
        }
    }
}
