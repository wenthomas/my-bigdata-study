package com.wenthomas.alogorithms.linkedList

/**
 * @author Verno
 * @create 2020-04-11 11:33 
 */
object Josephu {
    def main(args: Array[String]): Unit = {
        startGame(7, 2, 3)
    }

    /**
     *
     * @param n 总共n个人
     * @param start 从第start个人开始数数
     * @param num 数到num的人枪毙
     */
    def startGame(n: Int, start: Int, num: Int) = {
        val list = new CircularLinkedList[Int]
        //初始化数据
        for (i <- 1 to n) {
            list.add(i)
        }

        var startNode = list.find(2).pre

        while (list.head != list.tail) {
            for (i <- 1 to num) {
                startNode = startNode.next
            }
            list.delete(startNode.value)
            print(startNode + "->")

            startNode = startNode.pre
        }
        startNode.value
    }
}
