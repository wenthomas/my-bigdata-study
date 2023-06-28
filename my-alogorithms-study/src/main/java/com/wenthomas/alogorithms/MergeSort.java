package com.wenthomas.alogorithms;

import java.util.Arrays;

/**
 * 归并排序
 * @author Verno
 * @create 2022-11-28 17:12
 */
public class MergeSort {
    private static void mergeSort(int[] arr, int left, int right, int[] temp) {
        //当拆分到只有一个元素的数组时，结束递归
        if (left == right) {
            return;
        }
        int mid = (left + right) >> 1;
        mergeSort(arr, left, mid, temp);
        mergeSort(arr, mid + 1, right, temp);

        //合并两个区间
        for (int i = left; i <= right; i++) {
            temp[i] = arr[i];
        }

        int i = left;
        int j = mid + 1;
        for (int k = left; k <= right; k++) {
            if (i == mid + 1) {
                arr[k] = temp[j];
                j++;
            } else if (j == right + 1) {
                arr[k] = temp[i];
                i++;
            } else if (temp[i] <= temp[j]) {
                arr[k] = temp[i];
                i++;
            } else {
                arr[k] = temp[j];
                j++;
            }
        }

    }

    public static int[] sortArr(int[] arr) {
        int len = arr.length;
        //temp需要从最外部传参，才能贯穿整个递归过程
        mergeSort(arr, 0, len -1, new int[len]);
        return arr;
    }

    public static void main(String[] args) {
        int[] array = {21, 34, 556, 12, 3324, 75, 1, 55, 343, 100};
        System.out.println(Arrays.toString(sortArr(array)));
    }
}
