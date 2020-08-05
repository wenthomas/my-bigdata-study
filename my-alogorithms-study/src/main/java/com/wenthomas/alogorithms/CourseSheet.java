package com.wenthomas.alogorithms;

/**
 * @author Verno
 * @create 2020-08-04 9:57
 */

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

/**
 * 你这个学期必须选修 numCourse 门课程，记为 0 到 numCourse-1 。
 *
 * 在选修某些课程之前需要一些先修课程。 例如，想要学习课程 0 ，你需要先完成课程 1 ，我们用一个匹配来表示他们：[0,1]
 *
 * 给定课程总量以及它们的先决条件，请你判断是否可能完成所有课程的学习？
 *
 */


/**
 * 算法流程：
 *
 * 1、在开始排序前，扫描对应的存储空间（使用邻接表），将入度为 00 的结点放入队列。
 *
 * 2、只要队列非空，就从队首取出入度为 00 的结点，将这个结点输出到结果集中，
 * 并且将这个结点的所有邻接结点（它指向的结点）的入度减 11，在减 11 以后，如果这个被减 11 的结点的入度为 00 ，就继续入队。
 *
 * 3、当队列为空的时候，检查结果集中的顶点个数是否和课程数相等即可。
 *
 */

public class CourseSheet {
    public static void main(String[] args) {
        int[][] prerequisites = {{0,1}, {1,2}};

        System.out.println(canFinish(3, prerequisites));

    }

    public static boolean canFinish(int numCourses, int[][] prerequisites) {
        //特殊情况
        if (numCourses <= 0) {
            return false;
        }

        int pLen = prerequisites.length;
        if (pLen == 0) {
            return true;
        }

        //创建两个辅助数据结构
        //入度数组：通过结点的索引，我们能够得到指向这个结点的结点个数。
        int[] inDegree = new int[numCourses];
        //邻接表：通过结点的索引，我们能够得到这个结点的后继结点；使用HashSet目的是为了去重。
        HashSet<Integer>[] adj = new HashSet[numCourses];
        for (int i = 0; i < numCourses; i++) {
            adj[i] = new HashSet<>();
        }

        for (int[] p : prerequisites) {
            //每遍历一个课程组合，则第一课程的入度+1
            inDegree[p[0]] += 1;
            //每遍历一个课程组合，则第二课程则加一个后继结点
            adj[p[1]].add(p[0]);
        }

        //队列
        Queue<Integer> queue = new LinkedList<>();

        //1,首先加入入度为0的结点
        for (int i = 0; i < numCourses; i++) {
            if (inDegree[i] == 0) {
                queue.add(i);
            }
        }

        //2,记录已经出队的课程数量
        int cnt = 0;
        while (! queue.isEmpty()) {
            Integer top = queue.poll();
            cnt += 1;
            //遍历当前出队结点的所有后继结点
            for (Integer successor : adj[top]) {
                inDegree[successor] -= 1;
                //当有后继结点的入度减为0，则加入队列
                if (inDegree[successor] == 0) {
                    queue.add(successor);
                }
            }
        }

        //当出队课程数量=总课程量时，则表示该图没有环，不会造成冲突
        return cnt == numCourses;
    }
}
