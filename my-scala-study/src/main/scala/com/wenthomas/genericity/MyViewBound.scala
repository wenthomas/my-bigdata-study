package com.wenthomas.genericity

/**
 * @author Verno
 * @create 2020-03-10 22:24 
 */
/*
视图绑定:
    viewBound
    T <% Ordered[T]
    表示一定要存在一个隐式转换函数
        T => Ordered[T]
        Int => Ordered[Int]

    def max[T](x: T, y: T)(implicit ev$1: T => Ordered[T])

 */
object MyViewBound extends App {

    def myMax[T](x: T, y: T)(implicit ev$1: T => Ordered[T]): T = {
        if (x > y) x else y
    }

/*    //错误示范：因为String类型无法通过  > ， < 来比较大小，所以需要泛型T混入Ordered来实现比较方法（自动实现）
        ，使用视图绑定来实现
    def myMax(i: String, i1: String): String = {
        if (x > y) x else y
    }*/

    println(myMax(10, 20))
    println(myMax("b", "a"))

}
