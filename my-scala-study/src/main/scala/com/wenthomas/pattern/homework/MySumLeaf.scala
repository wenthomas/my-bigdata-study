package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 1:22 
 */
/**
 * 我们可以用列表制作只在叶子节点存放值的树。举例来说，列表((3 8) 2 (5)) 描述的是如下这样一棵树：
 *          *
 *         /|\
 *        * 2 *
 *       /\   |
 *      3  8  5
 * List[Any] = List(List(3, 8), 2, List(5))
 * 不过，有些列表元素是数字，而另一些是列表。在Scala中，你必须使用List[Any]。
 * 编写一个leafSum函数，计算所有叶子节点中的元素之和.
 */
object MySumLeaf extends App {

    val list:List[Any] = List(List(3, 8), 2, List(5))

    //使用递归完成
    def leafSum(list: List[Any]): Int = {
        var sum = 0
        list.foreach(x => {
            x match {
                case list:List[_] => sum += leafSum(list)
                case i:Int => sum += i
            }
        })
        sum
    }

    println(leafSum(list))

}
