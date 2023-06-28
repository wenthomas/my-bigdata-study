package com.wenthomas.collections.homework

import org.apache.commons.lang3.StringUtils

/**
 * @author Verno
 * @create 2020-03-08 15:22 
 */
/**
 * 编写一个函数, 接收一个字符串, 返回一个 Map.
 * 比如: indexes("Helloee")
 *
 * 返回: Map(H->{0}, e -> {1, 5, 6}, ...)   数字其实是下标
 */
object MapIndexes extends App {

    println(indexes("Helloee332"))

    def indexes(arg: String) = {
        //校验空字符串
        if (StringUtils.isBlank(arg)) Map()

        // List(H, e, l, l, o, e, e)
        val list = arg.toList

        list.zipWithIndex.groupBy(x => x._1).mapValues(x => x.map(x => x._2))
    }
}
