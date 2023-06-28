package com.wenthomas.alogorithms;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Verno
 * @create 2022-12-02 16:39
 */
public class LRUCache extends LinkedHashMap<Integer, Integer> {

    private int capacity;

    public LRUCache() {

    }

    //这里的 accessOrder 默认是为false，如果要按读取顺序排序需要将其设为 true
    //initialCapacity 代表 map 的 容量，loadFactor 代表加载因子 (默认即可)
    public LRUCache(int capacity) {
        super(capacity, 0.75F, true);
        this.capacity = capacity;
    }

    //根据题目要求，如果存在关键字key存在于缓存中，则返回value，否则返回-1。
    //这里我们使用LinkedHashMap的getOrDefault方法可以满足需求
    public int get(int key) {
        return super.getOrDefault(key, -1);
    }

    public void put(int key, int value) {
        super.put(key, value);
    }

/*    移除最近最少被访问条件之一，通过覆盖此方法可实现不同策略的缓存
    LinkedHashMap是默认返回false的，我们可以继承LinkedHashMap然后复写该方法即可
    例如 LeetCode 第 146 题就是采用该种方法，直接 return size() > capacity,
    条件就是 map 的大小不超过 给定的容量，超过了就得使用 LRU 了*/
    @Override
    protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
        return size() > capacity;
    }
}
