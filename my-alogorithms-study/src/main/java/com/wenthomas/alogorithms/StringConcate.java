package com.wenthomas.alogorithms;

import org.junit.Test;

/**
 * @author Verno
 * @create 2020-08-03 11:20
 */

/**
 * 给定两个字符串形式的非负整数 num1 和num2 ，计算它们的和。
 *
 * 注意：
 *
 * num1 和num2 的长度都小于 5100.
 * num1 和num2 都只包含数字 0-9.
 * num1 和num2 都不包含任何前导零。
 * 你不能使用任何內建 BigInteger 库， 也不能直接将输入的字符串转换为整数形式。
 *
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode-cn.com/problems/add-strings
 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 */
public class StringConcate {
    public static void main(String[] args) {
        String num1 = "1009";
        String num2 = "992";
        System.out.println(addStrings(num1, num2));
    }

    public static String addStrings(String num1, String num2) {
        int i = num1.length() - 1;
        int j = num2.length() - 1;
        int add = 0;
        StringBuffer str = new StringBuffer();
        while (i >= 0 || j >= 0 || add != 0) {
            //分别提取出num1和num2末位的数值
            int x = i >= 0? num1.charAt(i) - '0' : 0;
            int y = j >= 0? num2.charAt(j) - '0' : 0;
            //末位求和
            int result = x + y + add;
            //分别求出当前位的和以及是否进位
            str.append(result % 10);
            add = result / 10;
            i--;
            j--;
        }
        //计算完成后翻转运算结果
        str.reverse();
        return str.toString();
    }

    @Test
    public void test() {
        int a = '7';
        int b = '0';
        System.out.println( a - b);
    }
}
