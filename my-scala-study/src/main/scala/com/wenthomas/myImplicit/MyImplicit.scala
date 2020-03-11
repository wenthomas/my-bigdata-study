package com.wenthomas.myImplicit

import java.time.{LocalDate}

/**
 * @author Verno
 * @create 2020-03-06 14:34 
 */
/**
 * 隐式转换：隐式函数、隐式类、隐式参数和隐式值
 */
object MyImplicit extends App {

    /**
     * 隐式函数
     */
    val ago = "ago"
    val later = "later"
    //通过隐式函数实现 Int 到 自定义LocalDt类的隐式转换，然后再自动寻找调用LocalDt的days方法
    implicit def IntToLocalTime(num: Int) = new LocalDt(num)

    println(2.days(later))
    println(3 days ago)
    println("-----------------------------------------")




    /**
     * 隐式类：
     * 1，隐式类不能至于顶级类，隐式类必须为内部类
     * 2，主构造器必须有参数
     */

    println(10.compMax(9))
    println(10.compMin(9))

    implicit class MyRichInt(arg: Int) {
        def compMax(i: Int): Int = {
            if (arg > i) arg else i
        }

        def compMin(i: Int): Int = {
            if (arg < i) arg else i
        }
    }

    println("-----------------------------------------")







    /**
     * 隐式参数和隐式值：
     *  当调用函数的时候, 如果不传参数, 并且省略括号, 就会找隐式值!(只看类型,不看名字)
     * 注意：如果定义了隐式参数, 则整个参数列表中所有的参数都是隐式参数
     */
    //隐式参数
    implicit val wenthomas: String = "wenthomas"

    def showName(implicit arg0:String, arg1:String, arg2: String = "Verno") = {
        println(arg0)
        println(arg1)
        println(arg2)
    }

    showName

}

class LocalDt(num: Int) {
    def days(when: String): String = {
        if (when == "ago") LocalDate.now().plusDays(- num).toString
        else if (when == "later") LocalDate.now().plusDays( + num).toString
        else null
    }
}
