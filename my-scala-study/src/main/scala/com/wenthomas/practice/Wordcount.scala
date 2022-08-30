package com.wenthomas.practice

/**
 * @author Verno
 * @create 2022-08-30 18:58 
 */
object Wordcount {
    def main(args: Array[String]): Unit = {
        // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
        val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")

        val wordList = stringList.flatMap(str => str.split(" "))

        val wordMap = wordList.groupBy(x => x)

        println(wordMap)

        val wordCountMap = wordMap.map(tuple => (tuple._1, tuple._2.size))

        println(wordCountMap)

        val sortList = wordCountMap.toList.sortWith((x, y) => x._2 > y._2)

        println(sortList.take(3))
    }
}
