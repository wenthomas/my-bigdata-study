package com.wenthomas.alogorithms.hash

import com.wenthomas.alogorithms.linkedList.DoublyLinkedList

/**
 * @author Verno
 * @create 2020-04-13 10:14 
 */
/**
 * 自定义哈希表
 * 底层：数组 + 链表
 */
object HashDemo {
    def main(args: Array[String]): Unit = {
        val hash = new HashTable[Int]
        hash.add(10)
        hash.add(40)
        hash.add(60)
        hash.add(63)
        hash.add(78)
        hash.printAll
    }
}

class HashTable[T] {
    val initSize = 10
    val arr = new Array[DoublyLinkedList[T]](initSize)
    def add(ele: T) = {
        //找到ele应该去的那个链表所在的数组中的索引
        val index = ele.hashCode().abs % initSize
        //如果是第一次在这个位置添加元素，应该先创建链表
        if (arr(index) == null) arr(index) = new DoublyLinkedList[T]

        arr(index).add(ele)
    }


    def printAll = {
        for (i <- 0 until arr.length) {
            val list = arr(i)
            print(s"$i : ")
            if (list != null) {
                list.printAll
            }
            println
        }
    }
}
