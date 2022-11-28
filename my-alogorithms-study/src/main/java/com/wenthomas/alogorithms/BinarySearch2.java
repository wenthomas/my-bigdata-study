package com.wenthomas.alogorithms;

import java.util.Arrays;
import java.util.List;

/**
 * 二分查找
 * @author Verno
 * @create 2022-11-28 11:03
 */
public class BinarySearch2 {
    public static int binarySearch2(int[] array, int data) {
        if (null == array || array.length == 0) {
            return -1;
        }
        int length = array.length;
        int left = 0;
        int right = length - 1;
        int mid;
        //转变为有序序列
        BubbleSort.bubbleSort(array);
        while (left <= right) {
            //mid = (left + right)/2;
            //无符号右移，效率比较高
            mid = (left + right) >>> 1;
            if (array[mid] == data) {
                return mid;
            } else if (array[mid] < data) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return -1;
    }

    public static void main(String[] args) {
        int[] array = {45, 123, 23, 56, 77, 232, 786, 45, 87};
        //输出的为有序序列的index，二分查找的前提是有序序列
        System.out.println(binarySearch2(array,45));
    }
}
