package com.wenthomas.alogorithms.tree

/**
 * @author Verno
 * @create 2020-04-13 11:31 
 */
object BinaryTreeDemo {
    def main(args: Array[String]): Unit = {
        val root = new BinaryTree[Int](10)
        root.isRoot = true
        root.left = new BinaryTree[Int](9)
        root.right = new BinaryTree[Int](10)
        root.left.right = new BinaryTree[Int](11)
        root.preForeach(println)
    }
}

/**
 * 二叉树数据结构
 * @tparam T
 */
class BinaryTree[T](var value: T) {

    //左子树
    var left: BinaryTree[T] = _
    //右子树
    var right: BinaryTree[T] = _

    //是否为根节点：默认为false，需要时手动赋值
    var isRoot = false

    /**
     * 前序遍历：当前节点 -> 左 -> 右
     * @param op
     */
    def preForeach(op: T => Unit): Unit = {
        //先处理当前节点
        op(value)

        if (left != null) left.preForeach(op)
        if (right != null) right.preForeach(op)
    }

    /**
     * 中序遍历：左 -> 当前节点 -> 右
     * @param op
     */
    def infixForeach(op: T => Unit): Unit = {
        if (left != null) left.preForeach(op)
        op(value)
        if (right != null) right.preForeach(op)
    }

    /**
     * 后序遍历：左 -> 右 -> 当前节点
     * @param op
     */
    def postForeach(op: T => Unit): Unit = {
        if (left != null) left.preForeach(op)
        if (right != null) right.preForeach(op)
        op(value)
    }
}

/**
 * 二叉树节点
 * @tparam T
 */
class BinaryNode[T] {

    var left: BinaryNode[T] = _
    var right: BinaryNode[T] = _

    var value: T = _


}