package com.wenthomas.alogorithms;

/**
 * @author Verno
 * @create 2020-08-06 16:51
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * 给你一个字符串 s ，一个分割被称为 「好分割」 当它满足：将 s 分割成 2 个字符串 p 和 q ，它们连接起来等于 s 且 p 和 q 中不同字符的数目相同。
 *
 * 请你返回 s 中好分割的数目。
 *
 */
public class NumSplits {
    public static void main(String[] args) {
        System.out.println(numSplits2("acbadbaada"));
    }

    /**
     * 思路：暴力解法（双层for循环）
     * 1，左右两边维护一个哈希表，对字母计数
     * 2，只有当字母的计数从 0 变为 1 时，或者从 1 变为 0 时，字母个数才会变化
     * 3，左右两边字母个数相同时，就是好分割
     * 4，第一次 for 循环是初始化，第二次 for 是调整分割位置
     *
     * @param s
     * @return
     */
    public static int numSplits(String s) {
        //使用两个HashSet记录左右两个子字符串的不同字母个数
        HashSet<Character> left = new HashSet<>();
        HashSet<Character> right = new HashSet<>();
        int length = s.length();
        int result = 0;

        //遍历字符串s切割左右两边子字符串
        for (int i = 0; i < length; i++) {
            for (int j = 0; j <= i; j++) {
                left.add(s.charAt(j));
            }
            int leftCount = left.size();

            for (int k = i + 1; k < length; k++) {
                right.add(s.charAt(k));
            }
            int rightCount = right.size();

            //当左右两边子字符串的不同字符数相等时，结果+1
            if (leftCount == rightCount) {
                result += 1;
            }

            //初始化左右两边子字符串
            left.clear();
            right.clear();
        }

        return result;
    }

    /**
     * 优化算法：滑动窗口（一层for循环）
     * @param s
     * @return
     */
    public static int numSplits2(String s) {
        //使用两个HashSet记录左右两个子字符串的不同字母个数
        Map<Character, Integer> left = new HashMap();
        Map<Character, Integer> right = new HashMap();
        int length = s.length();
        int result = 0;

        //先将全部字符串归为右子字符串
        for (int i = 0; i < length; i++) {
            if (right.containsKey(s.charAt(i))) {
                right.put(s.charAt(i), right.get(s.charAt(i)) + 1);
            } else {
                right.put(s.charAt(i), 1);
            }
        }

        //从左往右滑动窗口
        for (int i = 0; i < length; i++) {
            if (left.containsKey(s.charAt(i))) {
                left.put(s.charAt(i), left.get(s.charAt(i)) + 1);
            } else {
                left.put(s.charAt(i), 1);
            }

            if (right.containsKey(s.charAt(i))) {
                if (right.get(s.charAt(i)) > 0) {
                    right.put(s.charAt(i), right.get(s.charAt(i)) - 1);
                }
                if (right.get(s.charAt(i)) == 0) {
                    right.remove(s.charAt(i));
                }
            }

            //判断左右两边子字符串不同字符数是否相等，是则result+1，否则继续
            if (left.keySet().size() == right.keySet().size()) {
                result += 1;
            }
        }

        return result;
    }
}
