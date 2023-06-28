package com.wenthomas.collections

import scala.collection.immutable.TreeMap
import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-19 18:00 
 */
object MyMap {
    def main(args: Array[String]): Unit = {
        var map1 = Map[String, Int]()
        var map2 = Map[String, Int](("tom", 25),("bbbb", 25))
        map1 += "verno" -> 23
        map1 ++= map2
        map1.foreach(println)

        //换成list就可以进行排序
        println(map1.toList.sortBy(_._1))

        val treeMap = TreeMap[String, Int](("tom", 27), ("bbbb", 25))

        println(treeMap)

        val map = "verno" -> 1 + "verno" -> 2 + "tom" -> 1
        // (((verno,1)verno,2)tom,1)
        println(map)
    }

}
