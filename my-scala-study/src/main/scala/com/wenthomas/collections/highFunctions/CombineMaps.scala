package com.wenthomas.collections.highFunctions

/**
 * @author Verno
 * @create 2020-03-07 17:43
 */
object CombineMaps extends App {
    // 合并两个Map
    private val map1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    private val map2 = Map("a" -> 10, "c" -> 30, "d" -> 40)
    /**
     * map：上一次折叠聚合的结果，类型是map
     *
     */
    private val map3 = map1.foldLeft(map2)((map, kv) => {
        map + (kv._1 -> (map.getOrElse(kv._1, 0) + kv._2 ))
    })
    println(map3)

/*    val map4 = map1 + ("d" -> 11)
    println(map4)*/
}
