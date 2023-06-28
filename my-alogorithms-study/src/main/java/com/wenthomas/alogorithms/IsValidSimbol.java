package com.wenthomas.alogorithms;

import java.util.Stack;

/**
 * 有效的括号:
 * 给定一个只包括 '('，')'，'{'，'}'，'['，']' 的字符串，判断字符串是否有效。
 * 有效字符串需满足：
 *     1. 左括号必须用相同类型的右括号闭合。
 *     2. 左括号必须以正确的顺序闭合。
 * 注意空字符串可被认为是有效字符串。
 *
 * tips：使用堆栈的方法来实现
 * @author Verno
 * @create 2022-12-02 19:08
 */
public class IsValidSimbol {
    private static boolean isValid(String s) {
        if (null == s) {
            return false;
        }
        if (s.isEmpty()) {
            return true;
        }

        int len = s.length();
        Stack<Character> stack = new Stack<>();
        for (char c: s.toCharArray()) {
            //输入为左括号时，则将相应的右括号推入栈中，以便后面与右括号进行比较，判断是否为左右匹配
            //输入为右括号时，则直接进入最后一种情况进行处理
            if (c == '(') {
                stack.push(')');
            } else if (c == '[') {
                stack.push(']');
            } else if (c == '{') {
                stack.push('}');
                //根据堆栈先进后出的性质，当输入的为右括号时，会与栈顶进行比较，不相等时则表示左右括号不匹配，直接返回false
                //当输入为右括号但是堆栈为空时，也表示该右括号没有相应的左括号进行匹配，返回false
            } else if (stack.empty() || c != stack.pop()) {
                return false;
            }
        }

        //当遍历完所有括号后，堆栈为空，则表示所有左右括号都相互匹配，返回true,否则为false
        return stack.empty();
    }

    public static void main(String[] args) {
        System.out.println(isValid("()[]{}"));
        System.out.println(isValid("(]"));
        System.out.println(isValid("()"));
    }
}
