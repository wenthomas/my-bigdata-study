package com.wenthomas.alogorithms;

import java.util.Arrays;

/**
 * 冒泡排序
 * @author Verno
 * @create 2022-11-28 10:02
 */
public class BubbleSort {
    public static int[] bubbleSort(int[] array) {
        if (null != array) {
            int length = array.length;
            for (int i = 0; i < length -1; i++) {
                boolean flag = false;
                for (int j = 0; j < length - 1 - i; j++) {
                    if (array[j] > array[j + 1]) {
                        int temp = array[j];
                        array[j] = array[j + 1];
                        array[j + 1] = temp;
                        flag = true;
                    }
                }
                if (!flag) {
                    break;
                }
            }
            return array;
        }
        return null;
    }

    public static void main(String[] args) {
        int[] array = {3, 1, 5, 7, 4, 4};
        System.out.println("长度= " + array.length);

        System.out.println(Arrays.toString(bubbleSort(array)));

    }
}
