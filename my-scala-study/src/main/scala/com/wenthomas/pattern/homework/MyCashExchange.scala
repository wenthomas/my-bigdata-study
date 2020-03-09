package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 2:11 
 */
/**
 * 附加题(认为自己够牛逼的可以完成下):
 * 大公司面试题: 使用递归
 * 假设某国的货币有若干面值，现给一张大面值的货币要兑换成零钱，问有多少种兑换方式
 */
object MyCashExchange extends App {

    def myCashExchange(cash: Int, coins: List[Int]): Int = {
        val tuple = (cash, coins)
        tuple match {
            case (a, b) if a == 0 =>
                1
            case (a, b) if a <0 || b.size == 0 =>
                0
            case (a, b) =>
                myCashExchange(a, b.tail) + myCashExchange(a - b.head, b)
        }
    }

    val coins = List(1, 5, 10, 20, 50, 100);
    println(myCashExchange(100, coins))
}
