package com.wenthomas.alogorithms;

/**
 * 爬楼梯
 *
 * 动态规划：
 * f(x)=f(x−1)+f(x−2)
 * @author Verno
 * @create 2022-11-29 16:08
 */
public class ClimbStairs {
    private static int climbStairs(int n) {
        //初始化：
        //p、q 作为滚动数组
        //r:n时的方案数
        int p = 0;
        int q = 0;
        int r = 1;

        for (int i = 1; i <= n; i++) {
            p = q;
            q = r;
            r = p + q;
        }

        return r;
    }

    public static void main(String[] args) {
        System.out.println(climbStairs(4));
    }
}
