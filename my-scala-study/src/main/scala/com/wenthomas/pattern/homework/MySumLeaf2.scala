package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 1:32 
 */
/**
 * 如果使用样例会更好一些, 从二叉树开始
 * sealed abstract class BinaryTree
 * case class Leaf(value : Int) extends BinaryTree
 * case class Node(left: BinaryTree, right: BinaryTree) extends BinaryTree
 * 编写一个函数计算所有叶子节点的元素的和
 * val r = Node(Leaf(8), Node(Leaf(3), Leaf(9)))
 */
sealed abstract class BinaryTree
case class Leaf(value : Int) extends BinaryTree
case class Node(left: BinaryTree, right: BinaryTree) extends BinaryTree

object MySumLeaf2 extends App {
    val r = Node(Leaf(8), Node(Leaf(3), Leaf(9)))

    //使用递归实现
    def mySumLeaf3(tree: BinaryTree): Int = {

        def loop(n: BinaryTree): Int = {
            var sum = 0
            n match {
                case leaf: Leaf => sum += leaf.value
                case node: Node => sum += (loop(node.left) + loop(node.right))
                case _ => sum += 0
            }
            sum
        }
        loop(tree)
    }

    println(mySumLeaf3(r))
}
