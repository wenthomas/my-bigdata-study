package com.wenthomas.alogorithms.queue

import scala.reflect.ClassTag

/**
 * @author Verno
 * @create 2020-04-11 9:08 
 */
/**
 * 自定义循环队列
 */
object QueueDemo {

    def main(args: Array[String]): Unit = {
        val que = new ArrayQueue[Int](5)
        que.enqueue(10)
        que.enqueue(20)
        que.enqueue(30)
        que.dequeue()
        que.dequeue()
        que.dequeue()
        que.dequeue()
        println(que.toString)
    }
}

class ArrayQueue[T: ClassTag](val initSize: Int) {
    // 内置数组: 存储数据(如果要使用泛型数组，则需要给T添加上下文)
    val arr = new Array[T](initSize)

    //队列头部
    var head = 0

    //队列尾部：指向队列中下一个要添加的元素的位置
    var tail = 0

    //队列中元素的个数: 用来判断队列是满或是空 
    var count = 0

    /**
     * 判断队列是否为空
     * @return
     */
    def isEmpty = count == 0

    /**
     * 判断队列是否已满
     * @return
     */
    def isFull = count == initSize

    /**
     * 入队操作
     * @param ele
     */
    def enqueue(ele: T) = {
        if (isFull) throw new UnsupportedOperationException("队列已满，不能再添加元素...")

        //如果没有满，则入队，把新入队的元素放在tail的位置
        arr(tail) = ele

        //tail应该更新
        tail += 1

        //队列中的元素个数也要更新
        count += 1

        //已经到最后一个位置，那下次的元素去数组头部
        if (tail == initSize) tail = 0

        println(s"入队操作：${ele}")
    }

    /**
     * 出队操作
     */
    def dequeue() = {
        //如果空队列则出队元素为:None
        if (isEmpty) {
            println("队列为空，出队为None...")
            None
        } else {
            //把队头元素弹出
            val result = arr(head)
            println(s"出队操作：${result}")
            head += 1
            //总数更新
            count -= 1

            if (head == initSize) head = 0
            Some(result)
        }
    }

    //todo:解决队列元素输出bug
    override def toString: String = {
        val result = new ArrayQueue[T](count)


        null
    }
}