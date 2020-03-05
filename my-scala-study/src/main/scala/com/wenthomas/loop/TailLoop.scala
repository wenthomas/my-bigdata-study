package com.wenthomas.loop

import scala.annotation.tailrec

/**
 * @author Verno
 * @create 2020-03-05 23:24 
 */
/**
 * 尾递归实操
 */
object TailLoop {
    def main(args: Array[String]): Unit = {
        println(func(5, 1))
        println(func1(5))
    }

    /**
     * 递归函数：尾递归---在递归的时候, 只有递归, 没有任何其他的运算, 这就是尾递归，需要实现一个累加器
     */
    @tailrec
    def func(base: Int, acc: BigInt): BigInt = {
        if (base == 1) acc
        else func(base - 1, base * acc)
    }

    /**
     * 递归函数：求5！
     * 注意：由于递归过程中掺杂了乘法运算，所以不能算作尾递归
     * @param base
     * @return
     */
    def func1(base: Int): BigInt = {
        if (base == 1) 1
        else base * func1(base - 1)
    }

}