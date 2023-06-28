package com.wenthomas.alogorithms.trie;

/**
 * 前缀树（数据结构：每个节点都有26个子节点的数结构）
 * @author Verno
 * @create 2022-12-07 19:03
 */
public class TrieNode {
    //树的末节点
    boolean val;
    //每个树都有26个子节点
    TrieNode[] children;

    public TrieNode() {
        this.val = false;
        this.children = new TrieNode[26];
    }
}
