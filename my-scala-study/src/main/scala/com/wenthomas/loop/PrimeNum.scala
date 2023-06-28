package com.wenthomas.loop

import scala.util.control.Breaks.{break, breakable}
/**
 * @author Verno
 * @create 2020-03-03 15:44 
 */
object PrimeNum {
    def main(args: Array[String]): Unit = {
        isPrimeNum(7)
    }

    /**
     * 判断一个整数是否为质数
     * @param num
     * @return
     */
    def isPrimeNum(num: Int): Boolean = {
        var isPrime = true
        //校验
        if (num <= 1) {
            throw new IllegalArgumentException("非法参数")
        } else if (num == 2) {
            //println(s"$num 是质数")
            return true
        }

        breakable {
            for (i <- 2 until num) {
                if (num % i == 0) {
                    isPrime = false
                    break
                }
            }
        }

        if (isPrime == true) {
            //println(s"$num 是质数")
            return true
        } else {
            //println(s"$num 不是质数")
            return false
        }

    }
}
