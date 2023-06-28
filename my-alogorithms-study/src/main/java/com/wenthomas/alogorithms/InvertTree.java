package com.wenthomas.alogorithms;

/**
 * 翻转二叉树
 * @author Verno
 * @create 2022-12-02 8:57
 */
public class InvertTree {
    private static TreeNode invertTree(TreeNode root) {
        //递归函数的终止条件：节点为空时返回
        if (null == root) {
            return null;
        }

        //当前节点的左右子树交换
        TreeNode tmp = root.right;
        root.right = root.left;
        root.left = tmp;

        //递归交换当前节点的左子树
        invertTree(root.left);
        //递归交换当前节点的右子树
        invertTree(root.right);

        //函数返回时就表示当前这个节点，以及它的左右子树都已经交换完了
        return root;
    }

    public static void main(String[] args) {
        TreeNode root = new TreeNode(4, new TreeNode(2, new TreeNode(1), new TreeNode(3)), new TreeNode(7, new TreeNode(6), new TreeNode(9)));
        TreeNode newTree = invertTree(root);
        System.out.println("sadsad");
    }
}
