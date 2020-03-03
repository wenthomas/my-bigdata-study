package com.wenthomas.function

/**
 * @author Verno
 * @create 2020-03-03 16:10 
 */
object HighFunctions {
    def main(args: Array[String]): Unit = {
        val arr1 = Array(30, 5, 7, 60, 1, 20)
        val add = (x: Int) => x + 2

        val add1: (Int,Int) =>Int = _ + _
        val multi: (Int,Int) =>Int = _ * _

        //模拟map实现
        foreach(map(arr1,add), println)
        println("------------------------------------------")


        //模拟filter过滤：将大于10的数筛选出来
        foreach(filter(arr1,(x:Int) => x > 10), println)
        println("------------------------------------------")

        //模拟reduce聚合
        println(reduce(arr1, multi))
        println("------------------------------------------")

    }

    def foreach(arr: Array[Int], op: Int => Unit) = {
        for (elem <- arr) {
            op(elem)
        }
    }

    /**
     * 模拟map映射处理
     * @param arr
     * @param op
     * @return
     */
    def map(arr: Array[Int], op:Int => Int) = {
        for (elem <- arr) yield op(elem)
    }

    /**
     * 模拟过滤器操作
     * @param arr
     * @param condition
     */
    def filter(arr: Array[Int], condition: Int => Boolean) = {
        for (elem <- arr if condition(elem)) yield elem
    }


    /**
     * 模拟reduce聚合操作
     * @param arr
     * @param op
     * @return
     */
    def reduce(arr: Array[Int], op: (Int,Int) => Int) = {
        var lastReduce = arr(0)
        for (i <- 1 until arr.length) {
            lastReduce = op(lastReduce,arr(i))
        }
        lastReduce
    }
}
