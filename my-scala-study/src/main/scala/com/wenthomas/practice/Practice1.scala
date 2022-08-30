package com.wenthomas.practice

import scala.collection.mutable

/**
 * @author Verno
 * @create 2022-08-29 18:41 
 */
object Practice1 {
    def main(args: Array[String]): Unit = {
        def test(i: Int): Int = {
            if (i == 1) 1 else {
                i * test(i - 1)
            }
        }

        println("结果：" + test(5))

        val list1 = List(1,2,3,4,5)
        val list2 = List(7,8,9)

        val list3 = list1:::list2
        println(list3.sortWith(_ > _))
        println(list3)

        val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
        println(nestedList.flatten)
        println(list3.groupBy(x => x % 2 == 0))
    }
}
