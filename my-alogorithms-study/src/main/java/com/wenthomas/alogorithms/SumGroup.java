package com.wenthomas.alogorithms;

/**
 * @author Verno
 * @create 2020-08-06 16:31
 */

import java.util.HashMap;
import java.util.Map;

/**
 * 给定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那 两个 整数，并返回他们的数组下标。
 *
 * 你可以假设每种输入只会对应一个答案。但是，数组中同一个元素不能使用两遍。
 *
 */
public class SumGroup {
    public static void main(String[] args) {
        int[] nums = new int[] {2, 7, 11, 15};
        int target = 9;
        int[] sum = twoSum(nums, target);
        if (sum.length != 0) {
            for (int i : sum) {
                System.out.println(i + " ");
            }
        }
    }

    public static int[] twoSum(int[] nums, int target) {
        //创建一张哈希表用于存放数据nums
        Map<Integer, Integer> map = new HashMap<>();

        //遍历nums，如果在hash表中找到target - nums那个元素，则直接返回，否则添入hash表中
        for (int i = 0; i < nums.length; i++) {
            int complement = target - nums[i];
            if (map.containsKey(complement)) {
                return new int[] {map.get(complement), i};
            }
            map.put(nums[i], i);
        }

        //找不到相应组合
        return new int[]{};
    }
}
