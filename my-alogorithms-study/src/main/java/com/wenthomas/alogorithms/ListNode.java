package com.wenthomas.alogorithms;

/**
 * 单链表
 * @author Verno
 * @create 2022-12-02 11:07
 */
public class ListNode {
    int val;
    ListNode next;
    ListNode() {

    }
    ListNode(int val) {
        this.val = val;
    }
    ListNode(int val, ListNode next) {
        this.val = val;
        this.next = next;
    }
}
