package com.wenthomas.alogorithms;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 两数之和
 * 给定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那 两个 整数，并返回他们的数组下标。
 * 你可以假设每种输入只会对应一个答案。但是，你不能重复利用这个数组中同样的元素。
 * ```
 * 给定 nums = [2, 7, 11, 15], target = 9
 *
 * 因为 nums[0] + nums[1] = 2 + 7 = 9
 * 所以返回 [0, 1]
 * ```
 *
 * @author Verno
 * @create 2022-11-29 11:22
 */
public class TwoSums {
    private static int[] twoSums(int[] nums, int target) {
        //创建一张hash表用于存放数据nums
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i <= nums.length; i++) {
            int complement = target - nums[i];
            if (map.containsKey(complement)) {
                return new int[]{map.get(complement), i};
            }
            map.put(nums[i], i);
        }

        //找不到则输出空数组
        return new int[]{};
    }

    public static void main(String[] args) {
        int[] nums = {2, 7, 11, 15};
        int target = 18;
        System.out.println(Arrays.toString(twoSums(nums, target)));

    }
}
