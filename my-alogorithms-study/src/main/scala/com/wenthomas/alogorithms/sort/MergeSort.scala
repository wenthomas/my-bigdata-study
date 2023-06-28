package com.wenthomas.alogorithms.sort

import com.wenthomas.alogorithms.sort.QuickSort.{randomArr, sort}

/**
 * @author Verno
 * @create 2020-04-11 16:04 
 */
/**
 * 归并排序：适用于分布式集群使用
 */
object MergeSort {
    def main(args: Array[String]): Unit = {
        val arr = randomArr()
        mergeSort(arr)
        println(arr.take(100).mkString(","))
    }

    def mergeSort(arr: Array[Int]) = {

        def merge(array: Array[Int], start: Int, mid: Int, stop: Int) = {
            //截取已经有序的数组[start, mid]
            val left = arr.slice(start, mid + 1)
            val right = arr.slice(mid + 1, stop + 1)

            var leftIndex = 0
            var rightIndex = 0
            for (i <- start to stop) {
                //如果左边已经取完，直接取右值，如果右边已经取完，直接取左值
                if (leftIndex == left.length) {
                    arr(i) = right(rightIndex)
                    rightIndex += 1
                } else if (rightIndex == right.length) {
                    arr(i) = left(leftIndex)
                    leftIndex += 1
                } else if (left(leftIndex) <= right(rightIndex)) {
                    arr(i) = left(leftIndex)
                    leftIndex += 1
                } else {
                    arr(i) = right(rightIndex)
                    rightIndex += 1
                }
            }
        }

        /**
         * merge优化：哨兵
         * @param array
         * @param start
         * @param mid
         * @param stop
         */
        def merge1(array: Array[Int], start: Int, mid: Int, stop: Int) = {
            //截取已经有序的数组[start, mid]
            val left = arr.slice(start, mid + 1) :+ Int.MaxValue
            val right = arr.slice(mid + 1, stop + 1) :+ Int.MaxValue

            var leftIndex = 0
            var rightIndex = 0
            for (i <- start to stop) {
                if (left(leftIndex) <= right(rightIndex)) {
                    arr(i) = left(leftIndex)
                    leftIndex += 1
                } else {
                    arr(i) = right(rightIndex)
                    rightIndex += 1
                }
            }
        }

        def sort(arr: Array[Int], start: Int, stop: Int): Unit = {
            if (start >= stop) return

            //1,分
            val mid = (start + stop) / 2

            //对左边排序
            sort(arr, start, mid)

            //对右边排序
            sort(arr, mid + 1, stop)

            //2,合并
            merge(arr, start, mid, stop)
        }

        sort(arr, 0, arr.length - 1)
    }
}
