package com.wenthomas.collections.wordCount

/**
 * @author Verno
 * @create 2020-03-07 22:41 
 */
object MyCount2 extends App {
    val tupleList = List(
        ("Hello hello Scala Spark World", 4),
        ("Hello Scala Spark", 3),
        ("Hello Scala", 2),
        ("Hello", 1))

    //方法一：先展开为一个长字符串集合再做计数聚合
    private val wordCount1: Any = tupleList.map { case (k, v) => (k + " ") * v }
            .flatMap(x => x.split(" "))
            .groupBy(x => x)
            .mapValues(x => x.length)

    println(wordCount1)
    println("---------------------------------------------------")

    //方法二：以每个单词与出现频次先组成各个小元组，再进行分组合并操作
    //使用嵌套算子： flatMap内再调用一层map，mapValues内再调用一层foldLeft
    private val wordCount2: Any = tupleList.flatMap { case (k, v) => k.split(" ").map(x => (x, v)) }
            .groupBy(kv => kv._1.toLowerCase)
            .mapValues(list => list.foldLeft(0)((x, y) => x + y._2))

    println(wordCount2)

}
