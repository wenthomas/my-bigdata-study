package com.wenthomas.alogorithms;

/**
 * 翻转链表
 *
 * 递归函数
 * @author Verno
 * @create 2022-12-02 12:13
 */
public class ReverseList {
    private static ListNode reverseList(ListNode head) {
        //递归程序退出条件
        if (null == head || head.next == null) {
            return head;
        }

        //递归
        //使用递归函数，一直递归到链表的最后一个结点，该结点就是反转后的头结点，记作 ret
        ListNode ret = reverseList(head.next);
        //此后，每次函数在返回的过程中，让当前结点的下一个结点的 next 指针指向当前节点
        head.next.next = head;
        //同时让当前结点的 next 指针指向 NULL ，从而实现从链表尾部开始的局部反转
        head.next = null;

        return ret;
    }

    public static void main(String[] args) {

    }
}
