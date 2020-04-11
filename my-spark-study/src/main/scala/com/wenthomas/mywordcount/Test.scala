package com.wenthomas.mywordcount

import scala.collection.mutable.ArrayBuffer

/**
 * @author Verno
 * @create 2020-03-12 11:04 
 */

object Test {

    def main(args: Array[String]): Unit = {
        val test = new MyTest
        println(math.sqrt(2))
        println("Hello".take(1))
        println("Hello".reverse(0))
        val arr01 = ArrayBuffer[Any](1, 2, 3,4,5)
        arr01.trimEnd(3)
        println(arr01)
        println(Array(1, 7, 2, 9).sorted.mkString(","))

    }

}
class MyTest{
    val a = "23"
    var b = "45"
}
