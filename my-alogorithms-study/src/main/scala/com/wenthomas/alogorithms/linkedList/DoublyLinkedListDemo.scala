package com.wenthomas.alogorithms.linkedList

/**
 * @author Verno
 * @create 2020-04-11 10:19 
 */
/**
 * 自定义双向链表
 */
object DoublyLinkedListDemo {
    def main(args: Array[String]): Unit = {

    }
}

class DoublyLinkedList[T] {
    //Node用来记录单向链表中的节点信息
    case class Node(value: T, var pre: Node, var next: Node) {
        override def toString: String = value.toString
    }

    //记录链表的头节点
    var head: Node = _

    //（可选）为了方便添加元素时快速找到链表尾部位置
    var tail: Node = _

    def add(ele: T) = {
        val newNode = Node(ele, null, null)

        if (head == null) {
            //第一次添加
            head = newNode
            tail = newNode
        } else {
            //tail的next指向新节点
            tail.next = newNode
            //新节点pre指向tail
            newNode.pre = tail
            //让tail指向新节点
            tail = newNode
        }
    }

    /**
     * 删除节点
     * @param ele
     * @return
     */
    def delete(ele: T) = {
        //先找到要删除的节点
        val targetNode = find(ele)
        if (targetNode == null) false
        else  {
            //待删除的元素的上一节点
            val pre = targetNode.pre

            //待删除的元素的下一节点
            val next = targetNode.next

            if (head == tail) {
                //原链表只有一个节点
                head = null
                tail = null
            } else if (targetNode == head) {
                //如果删除的是head，则不处理head.pre
                next.pre = null
                head = next
            } else if (targetNode == tail) {
                //如果删除的是head，则不处理head.next
                pre.next = null
                tail = pre
            } else {
                //删除
                pre.next = next
                next.pre = pre
            }
            true
        }

    }

    def find(ele: T): Node = {
        var temp = head
        while (head != null) {
            if (temp.value == ele) {
                //找到了
                return temp
            } else {
                //没找到，继续遍历
                temp = temp.next
            }
        }
        null
    }

    //todo
    def printAll = {
        if (head != null) {
            var temp = head
            do {
                print(temp.value + "->")
                temp = temp.next
            } while (temp != null)
        }
    }
}