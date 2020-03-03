package com.wenthomas.function

import com.wenthomas.loop.PrimeNum
/**
 * @author Verno
 * @create 2020-03-03 21:15 
 */
object PrimeNumSum {
    def main(args: Array[String]): Unit = {

        //初始化数据
        val arr = data(100, 10000)

        println(reduce(filter(arr, PrimeNum.isPrimeNum), _ + _))

    }


    /**
     * 获取从start到end之间的所有整数
     * @param start
     * @param end
     */
    def data(start: Int, end: Int) = {
        val arr = new Array[Int](end - start + 1)
        var index = 0
        for (i <- start to end) {
            arr(index) = i
            index += 1
        }
        arr
    }

    def filter(arr: Array[Int], condition: Int => Boolean) = {
        for (elem <- arr if condition(elem)) yield elem
    }

    def reduce(arr: Array[Int], op: (Int, Int) => Int) = {
        var lastReduce = arr(0)
        for (i <- 2 until arr.length) {
            lastReduce = op(lastReduce, arr(i))
        }
        lastReduce
    }
}
