package com.wenthomas.collections

/**
 * @author Verno
 * @create 2020-03-09 22:10 
 */
object MyStream extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)

    /**
     * Stream: 惰性数据结构，只有用到时才调取操作
     */
    // toStream：集合转化为Stream流
    private val stream: Stream[Int] = list1.toStream
    println(stream.head)    //30
    println(stream.tail.head)   //50
    println(stream)         //Stream(30, 50, ?)
    println(stream.take(3).force)   //强制求值：Stream(30, 50, 70, 60, 10, 20)，调用take(3)表示取前3个：Stream(30, 50, 70)

    println("-----------------------------------------------------------")

    /**
     * 菲波那切数列：Stream实现方式
     */
    def fibSeq(n: Int) = {
        //递归求集合: 每次递归为一个Stream流
/*        def loop(a: Int, b: Int): Stream[Int] = {
            a #:: loop(b, a + b)
        }
        loop(1,1).take(n).toList*/

        //简化写法：
        def loop: Stream[Int] = {
            1 #:: loop.scanLeft(1)(_ + _)
        }
        loop.take(n).toList
    }

    println(fibSeq(10))


}
