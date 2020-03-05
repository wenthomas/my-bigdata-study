package com.wenthomas.myWhile

import scala.annotation.tailrec

/**
 * @author Verno
 * @create 2020-03-05 23:17 
 */

/**
 * 使用scala来模拟Java语言中While循环
 */
object Loop {
    def main(args: Array[String]): Unit = {
        var a = 0
        myWhile(a <= 100) {
            println(a)
            a += 1
        }
    }

    @tailrec
    def myWhile(condition: => Boolean)(op: => Unit): Unit = {
        if (condition) {
            //执行目标动作
            op
            //递归调用myWhile
            myWhile(condition)(op)
        }
    }
}

