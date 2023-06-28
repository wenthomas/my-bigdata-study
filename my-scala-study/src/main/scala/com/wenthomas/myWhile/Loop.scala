package com.wenthomas.myWhile

import scala.annotation.tailrec

/**
 * @author Verno
 * @create 2020-03-05 23:17 
 */

/**
 * 使用scala来模拟Java语言中While循环：通过函数柯里化、高阶函数、控制抽象（名调用）来实现
 * 名调用：传递整个代码块，而不是传递值
 *      实现方法：参数列表写成 xxx: => Any
 *
 *      xxx: () => Any 这是函数类型
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

