package com.wenthomas.alogorithms.linkedList

/**
 * @author Verno
 * @create 2020-04-11 10:00 
 */
/**
 * 单向链表
 */
object SinglyLinkedListDemo {
    def main(args: Array[String]): Unit = {
        val list = new SinglyLinkedList[Int]
        list
    }
}

class SinglyLinkedList[T] {
    //Node用来记录单向链表中的节点信息
    case class Node(value: T, var next: Node)

    //记录链表的头节点
    var head: Node = _

    //（可选）为了方便添加元素时快速找到链表尾部位置
    var tail: Node = _

    //todo
    def contain(ele: T) = {
        if (head == null) {

        }
    }

    /**
     * 向链表中添加元素
     * @param ele
     */
    def add(ele: T) = {
        //把要添加的值封装为Node，它没有下一节点
        val newNode = Node(ele, null)

        if (head == null) {
            //第一次添加时，head和tail都是自己
            head = newNode
            tail = newNode
        } else {
            //将该值添加到原链表尾部位置
            tail.next = newNode

            //再让tail指向最后一个元素
            tail = newNode
        }
    }

    def delete(ele: T) = {

    }

    //todo
    def printAll = {
        if (head != null) {
            var temp = head
            do {
                println(temp.value)
                temp = temp.next
            } while (temp != null)
        }
    }
}
