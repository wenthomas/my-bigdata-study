package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 20:05 
 */
object MyListPatternTest extends App {


    val list1 = List(30, 50, 70, 60, 10, 20)

    //List(Some(30), None, None, None, Some(10), Some(20))
    private val options: List[Option[Int]] = list1.map(x => if (x < 40) Some(x) else None)

    def f(list: List[Option[Int]])={
        //模式匹配更简单

        //第二种
        list match {
            // a:List[Option[Int]]=>a.map(op=>op.get).foldLeft(0)((x, y) => x + y)
            case a:List[Option[Int]]=>a.map(op => op match {
                case Some(x) => x
                case None => 0
            }).foldLeft(0)(_ + _)
            case _=>0
        }
    }

    options match {
        case list:List[_] => println("This is a List")
        case _ => println("Other")
    }

    println(f(options))

}
