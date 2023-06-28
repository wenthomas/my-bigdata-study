package com.wenthomas.alogorithms;

/**
 * 编辑距离
 *
 * 给定两个单词 word1 和 word2，计算出将 word1 转换成 word2 所使用的最少操作数 。
 *
 * 你可以对一个单词进行如下三种操作：
 *     1. 插入一个字符
 *     2. 删除一个字符
 *     3. 替换一个字符
 *
 * tips:使用动态规划实现
 * @author Verno
 * @create 2022-12-08 19:00
 */
public class MinDistance {
    private static int minDistance(String word1, String word2) {
        if (null == word1 || null == word2) {
            return -1;
        }

        //构建动态规划的状态方程矩阵
        int n1 = word1.length();
        int n2 = word2.length();
        int[][] dp = new int[n1 + 1][n2 + 1];
        //第一行
        for (int j = 1; j <= n2; j++) {
            dp[0][j] = dp[0][j - 1] + 1;
        }
        //第一列
        for (int i = 1; i <= n1; i++) {
            dp[i][0] = dp[i - 1][0] + 1;
        }

        //补充完状态方程矩阵
        for (int i = 1; i <= n1; i++) {
            for (int j = 1; j <= n2; j++) {
                if (word1.charAt(i - 1) == word2.charAt(j - 1)) {
                    //特别地，如果 A 的第 i 个字符和 B 的第 j 个字符原本就相同，那么我们实际上不需要进行修改操作。
                    // 在这种情况下，dp[i][j] 最小可以为 dp[i-1][j-1]
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    //dp[i][j-1] 为 A 的前 i 个字符和 B 的前 j - 1 个字符编辑距离的子问题。
                    // 即对于 B 的第 j 个字符，我们在 A 的末尾添加了一个相同的字符，那么 dp[i][j] 最小可以为 dp[i][j-1] + 1；
                    //dp[i-1][j] 为 A 的前 i - 1 个字符和 B 的前 j 个字符编辑距离的子问题。
                    // 即对于 A 的第 i 个字符，我们在 B 的末尾添加了一个相同的字符，那么 dp[i][j] 最小可以为 dp[i-1][j] + 1；
                    dp[i][j] = Math.min(Math.min(dp[i - 1][j - 1], dp[i][j - 1]), dp[i - 1][j]) + 1;
                }
            }
        }

        //状态方程矩阵的最后一个坐标即为word1转换为word2的最小编辑距离
        return dp[n1][n2];
    }

    public static void main(String[] args) {
        System.out.println(minDistance("horse", "ros"));
        System.out.println(minDistance("intention", "execution"));
    }
}
