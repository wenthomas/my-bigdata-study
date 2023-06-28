package com.wenthomas.collections.highFunctions

/**
 * 筛选出数字部分并对每个数字进行乘方操作
 */
object Practice extends App {
    val list1: List[Any] = List(30, "aa", 70, false, 10, 20)

    /**
     * 1，先判断筛选出Int类型元素
     * 2，强制转换为Int类型
     * 3，通过隐式转换调用自定义的乘方方法得出结果
     */
    val list2 = list1.filter(_.isInstanceOf[Int])
            .map(_.asInstanceOf[Int])
            .map(_ ** 2)
    println(list2)

    /**
     * Int增强：新增乘方方法 **
     * @param i
     */
    implicit class RichInt(i:Int) {

        def **(arg: Int): Int = {
            math.pow(i, arg).toInt
        }

    }
}
