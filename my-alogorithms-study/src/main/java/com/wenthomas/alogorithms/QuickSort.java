package com.wenthomas.alogorithms;

import java.util.Arrays;

/**
 * 快速排序
 * tips:
 * 选定一个基准值pivot（通常指定为数组第一个值），比基准值大的放在右侧，比基准值小的放在左侧
 * 指定左右两个指针分别为left，right ；left < right
 * 指定函数退出条件，即left > right
 * 先从右向左遍历，即右指针向左移动——right–操作，发现比pivot小的值暂停移动
 * 再从左向右遍历，即左指针向右移动——left++操作，发现比pivot大的值暂停移动
 * 交换左右指针发现的两个值的位置
 * 当两指针相遇，即left == right，当前值与pivot交换位置
 *
 * 递归处理
 * ————————————————
 * @author Verno
 * @create 2022-11-28 14:48
 */
public class QuickSort {
    public static void quickSort(int[] array,int low, int high) {

        int right = high;
        int left = low;
        int pivot = array[low];
        int temp;

        while (left < right) {
            while (left < right && array[right] >= pivot) {
                right --;
            }
            while (left < right && array[left] <= pivot) {
                left ++;
            }
            temp = array[left];
            array[left] = array[right];
            array[right] = temp;

            if (left == right) {
                array[low] = array[left];
                array[left] = pivot;
            }
        }

        //递归左数组与右数组
        if (low < left) {
            quickSort(array, low, left - 1);
        }
        if (right < high) {
            quickSort(array, right + 1, high);
        }

    }


    public static void main(String[] args) {
        int[] array = {21, 34, 556, 12, 3324, 75, 1, 55, 343, 100};
        quickSort(array, 0, array.length - 1);
        System.out.println(Arrays.toString(array));
    }
}
