package com.wenthomas.alogorithms;

import java.util.Arrays;

/**
 *
 * 数组中的第K个最大元素
 * 基于快速排序实现
 * @author Verno
 * @create 2022-12-06 18:37
 */
public class FindKthLargest {
    private static int findKthLargest(int[] nums, int k) {
        if (null == nums || k > nums.length) {
            return 0;
        }
        int len = nums.length;

        //先对数组进行排序：使用快速排序
        quickSort(nums, 0, len - 1);

        //输出第K大的元素
        return nums[len - k];
    }

    /**
     * 快速排序
     * @param nums  输入需要排序的数组
     * @param low   数组起始坐标：0
     * @param high  数组最后坐标： length - 1
     */
    private static void quickSort(int[] nums, int low, int high) {
        int right = high;
        int left = low;
        int pivot = nums[low];
        int temp;

        while(left < right) {
            while (left < right && nums[right] >= pivot) {
                right--;
            }
            while (left < right && nums[left] <= pivot) {
                left++;
            }

            temp = nums[right];
            nums[right] = nums[left];
            nums[left] = temp;

            if (left == right) {
                nums[low] = nums[left];
                nums[left] = pivot;
            }
        }

        //递归左数组以及右数组
        if (low < left) {
            quickSort(nums, low, left - 1);
        }
        if (right < high) {
            quickSort(nums, right + 1, high);
        }
    }


    public static void main(String[] args) {
        int[] nums1 = {3, 2, 1, 5, 6, 4};
        int[] nums2 = {3,2,3,1,2,4,5,5,6};
        System.out.println(findKthLargest(nums1, 2));
        System.out.println(findKthLargest(nums2, 4));
    }
}
