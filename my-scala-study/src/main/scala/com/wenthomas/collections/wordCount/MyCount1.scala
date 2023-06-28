package com.wenthomas.collections.wordCount

import scala.io.Source

/**
 * @author Verno
 * @create 2020-03-07 22:27 
 */
object MyCount1 extends App {

    // 读取一个文件的内容, 统计这个文件中,每个单词出现的次数
    private val path = """E:\project\my-bigdata-study\my-scala-study\src\main\scala\com\wenthomas\collections\wordCount\MyCount1.scala"""

    // 1. 读文件内容, 放入到集合中   文件中的每一行
    private val lines: List[String] = Source.fromFile(path, "utf-8").getLines().toList

    // 2. 切割单词 使用非单词字符
    private val words: List[String] = lines.flatMap(x => x.split("\\W+")).filter(x => x.length > 0)

    // 3. 把相同的单词分组
    private val wordGrouped = words.groupBy(w => w)

    // 4. 进行map, 计算每个单词的个数
    private val wordCountMap: Any = wordGrouped.mapValues(x => x.size)
    println(wordCountMap)

}
