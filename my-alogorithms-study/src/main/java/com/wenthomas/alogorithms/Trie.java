package com.wenthomas.alogorithms;

/**
 * 实现Trie（前缀树）
 *
 * Trie（发音类似 "try"）或者说 前缀树 是一种树形数据结构，用于高效地存储和检索字符串数据集中的键。这一数据结构有相当多的应用情景，例如自动补完和拼写检查。
 *
 * 请你实现 Trie 类：
 *
 * Trie() 初始化前缀树对象。
 * void insert(String word) 向前缀树中插入字符串 word 。
 * boolean search(String word) 如果字符串 word 在前缀树中，返回 true（即，在检索之前已经插入）；否则，返回 false 。
 * boolean startsWith(String prefix) 如果之前已经插入的字符串 word 的前缀之一为 prefix ，返回 true ；否则，返回 false 。
 *
 *
 * @author Verno
 * @create 2022-12-07 18:38
 */
public class Trie {

    private TrieNode root;

    public Trie() {
        root = new TrieNode();
    }

    //void insert(String word) 向前缀树中插入字符串 word 。
    //优化：使用递归实现
    public void insert(String word) {
        TrieNode p = root;
        for (char c: word.toCharArray()) {
            //求出每个字母的坐标值
            int i = c - 'a';

            //当该节点为空，则可以插入字符
            if (null == p.children[i]) {
                p.children[i] = new TrieNode();
            }

            //下探到下一个节点
            p = p.children[i];

        }

        //当遍历完word，则val置true
        p.val = true;
    }

    //boolean search(String word) 如果字符串 word 在前缀树中，返回 true（即，在检索之前已经插入）；否则，返回 false 。
    //实现与insert类似
    //优化：使用递归实现
    public boolean search(String word) {
        TrieNode p = root;

        for (char c: word.toCharArray()) {
            int i = c - 'a';

            if (null == p.children[i]) {
                return false;
            }

            p = p.children[i];

        }
        return p.val;
    }

    //boolean startsWith(String prefix) 如果之前已经插入的字符串 word 的前缀之一为 prefix ，返回 true ；否则，返回 false 。
    //实现与search类似
    public boolean startWith(String prefix) {

        TrieNode p = root;

        for (char c: prefix.toCharArray()) {
            int i = c - 'a';

            if (null == p.children[i]) {
                return false;
            }

            p = p.children[i];
        }
        return true;
    }


}
