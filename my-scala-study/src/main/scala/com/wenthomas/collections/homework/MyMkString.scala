package com.wenthomas.collections.homework

import org.apache.commons.lang.StringUtils

/**
 * @author Verno
 * @create 2020-03-08 15:42 
 */
/**
 * 实现一个函数，作用与mkString相同，使用foldLeft完成
 */
object MyMkString extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)

    println(list1.myMkString("MyMkString: ", "-", "//"))

    implicit class RichCollections(it: List[Any]){
        def myMkString(arg0: String, arg1: String, arg2: String): String = {
            if (it.isInstanceOf[List[Any]]) it.asInstanceOf[List[Any]]

            //参数校验
            val head = if (StringUtils.isBlank(arg0)) "" else arg0
            val split = if (StringUtils.isBlank(arg1)) "" else arg1
            val tail = if (StringUtils.isBlank(arg2)) "" else arg2

            //先使用zipWithIndex为list1创建索引映射，再进行折叠拼串，遍历到最后一个元素时改为拼结尾符
            it.zipWithIndex.foldLeft(head)((line, i) => {
                if (i._2 == it.length - 1) line + i._1 + tail else line + i._1 + split
            })
        }
    }

}
