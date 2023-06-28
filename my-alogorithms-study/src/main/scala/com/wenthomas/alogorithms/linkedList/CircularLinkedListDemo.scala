package com.wenthomas.alogorithms.linkedList

/**
 * @author Verno
 * @create 2020-04-11 10:46 
 */
/**
 * 自定义双向循环链表
 */
object CircularLinkedListDemo {
    def main(args: Array[String]): Unit = {

    }
}

/**
 * 在自定义双向链表的基础上做改造
 * @tparam T
 */
case class CircularLinkedList[T]() extends DoublyLinkedList[T] {
    override def add(ele: T): Unit = {
        super.add(ele)

        //让head 和 tail形成一个环状
        head.pre = tail
        tail.next = head
    }

    override def find(ele: T): Node = {
        var temp = head
        while (head != null) {
            if (temp.value == ele) {
                //找到了
                return temp
            } else {
                //没找到，继续遍历
                temp = temp.next

                //查找的过程中，如果重新回到了头部，则证明没找到
                if (temp == head) return null
            }
        }
        null
    }

    override def delete(ele: T): Boolean = {
        if (super.delete(ele)) {
            //重新构建环
            if (head != null) head.pre = tail
            if (tail != null) tail.next = head
            true
        } else false
    }

    override def printAll: Unit = {
        if (head != null) {
            var temp = head
            do {
                println(temp.value)
                temp = temp.next
            } while (temp != null && temp != head)
        }
    }
}
