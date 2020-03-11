package com.wenthomas.single

/**
 * @author Verno
 * @create 2020-03-05 18:23 
 */
/**
 * 定义一个 Point 类和一个伴生对象,使得我们可以不用 new 而直接用 Point(3,4)来构造 Point 实例
 */
object Point {
    def apply(num1: Int, num2: Int): Point = new Point(num1, num2)

    def main(args: Array[String]): Unit = {
        Point(3, 4).toString
    }
}

class Point(num1: Int, num2: Int) {
    override def toString: String = {
        val value = s"num1=${num1},\nnum2=${num2}"
        println(value)
        value
    }
}