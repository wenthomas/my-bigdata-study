package com.wenthomas.alogorithms;


/**
 * 最长回文字符子串
 *
 * 给你一个字符串 s，找到 s 中最长的回文子串。
 * 使用动态规划实现
 * @author Verno
 * @create 2022-12-02 18:36
 */
public class LongestPalindrome {
    private static String longestPalindrome(String s) {
        if (null == s || s.length() < 2) {
            return s;
        }

        int strLen = s.length();
        //初始化最长回文子串的起止坐标及长度
        int maxStart = 0;
        int maxEnd = 0;
        int maxLen = 1;

        //我们用一个 boolean 类型的二维数组 dp[l][r] 表示字符串从 i 到 j 这段是否为回文
        boolean[][] dp = new boolean[strLen][strLen];

        //右坐标r的起始位置必须为1
        for (int r = 1; r < strLen; r++) {
            for (int l = 0; l < r; l++) {

                //当l,r所在字符相等，且上一次循环的状态dp为true，则置本次dp为true并且更新坐标及长度
                //r - l <= 2 表示的是当前字符串s为奇数字符串，或者当前dp为相邻字符串时的状态
                if (s.charAt(l) == s.charAt(r) && (r - l <= 2 || dp[l + 1][r - 1])) {
                    dp[l][r] = true;
                    if (r - l + 1 > maxLen) {
                        maxLen = r - l + 1;
                        maxStart = l;
                        maxEnd = r;
                    }
                }
            }
        }

        return s.substring(maxStart, maxEnd + 1);
    }

    public static void main(String[] args) {
        System.out.println(longestPalindrome("babad"));
        System.out.println(longestPalindrome("cbbd"));
        System.out.println(longestPalindrome("cbbdsdsddddd"));
        System.out.println(longestPalindrome("c"));
        System.out.println(longestPalindrome(""));
    }

}
