package com.wenthomas.practice

import scala.collection.mutable

/**
 * @author Verno
 * @create 2022-03-14 9:42 
 */
object Hello {
    def main(args: Array[String]): Unit = {
        println("Hello world!")

        var a: Int = 3
        var b: Int = 4
        var c = a + b * 4
        println("c = " + c)

        def f1(f: (Int, Int) => Int) : Int = {
            f(2, 4)
        }

        def add(a :Int, b :Int) = a + b

        println(f1(add))
        println(f1(add _))

        println("----------")
        def f = () => {
            println("fsdfsd")
            10
        }

        def foo(a: Int) = {
            println(a)
            println(a)
        }

        foo(f())
        println("----------------")

        val list: List[Int] = List(1,2,3,4)
        val list1 = 6::5::list

        list1.foreach(println)

        println("----------------")
        val map = mutable.Map( "a"->1, "b"->2, "c"->3 )
        val maybeInt = map.put("e", 4)
        println(maybeInt.getOrElse(2))

        val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
        println(nestedList.flatten)
    }




}
