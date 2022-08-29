package com.wenthomas.alogorithms.leetcode;

/**
 * @author Verno
 * @create 2022-03-17 16:50
 *
 * 给你两个 非空 的链表，表示两个非负的整数。它们每位数字都是按照 逆序 的方式存储的，并且每个节点只能存储 一位 数字。
 *
 * 请你将两个数相加，并以相同形式返回一个表示和的链表。
 *
 * 你可以假设除了数字 0 之外，这两个数都不会以 0 开头。
 *
 */
public class AddTwoNumbers {
    public static void main(String[] args) {
        ListNode listNode = new ListNode(2);
    }

}

class ListNode {
    private int val;
    private ListNode next;
    public ListNode() {}
    public ListNode(int val) {
        this.val = val;
    }
    public ListNode(int val, ListNode next) {
        this.val = val;
        this.next = next;
    }
}