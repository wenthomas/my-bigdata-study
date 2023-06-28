package com.wenthomas.alogorithms.tree

/**
 * @author Verno
 * @create 2020-04-13 13:57 
 */
/**
 * 自定义二叉搜索树
 */
object SearchBinaryTreeDemo {
    def main(args: Array[String]): Unit = {
        val arr = Array(5, 8, 3, 2, 6, 1, 10, 40, 23, 15, 37)
        val tree = new SearchBinaryTree[Int]

        arr.foreach(x => tree.add(x))

/*        tree.infixForeach(println)
        println(tree.search(65))*/
        tree.delete(1335)
        tree.infixForeach(println)
    }

}

/**
 * 二叉搜索树
 * @tparam T
 */
class SearchBinaryTree[T: Ordering] {
    //从冥界召唤Ordering[T]的隐式值当作比较器
    private val ord: Ordering[T] = implicitly[Ordering[T]]

    var root: TreeNode[T] = _

    def add(ele: T) = {
        //如果是第一次添加，自动设置为根节点
        if (root == null) root = new TreeNode[T](ele)
        else root.add(ele)
    }

    def delete(ele: T): Option[T] = {
        if (root == null) None
        else if (ord.equiv(ele, root.value)) {
            //删除的是根节点
            if (root.left == null && root.right == null) root = null
            else if (root.left != null && root.right!= null) {
                //左右节点都有的情况
                //删除根节点，把右节点最小的节点挪到当前根节点
                root.value = root.right.delete(root.right.min).get
            } else {
                //左节点或右节点存在的情况
                if (root.left != null || root.right == null) {
                    root = root.left
                    root.parent = null
                } else {
                    root = root.right
                    root.parent = null
                }
            }
            Some(ele)
        } else root.delete(ele)
    }

    def search(ele: T): TreeNode[T] = {
        //如果是空树，则返回null
        if (root == null) return null
        else root.search(ele)
    }

    /**
     * 二叉搜索树使用中序遍历比较合适
     * @param op
     */
    def infixForeach(op: T => Unit): Unit = {
        if (root != null) {
            if (root.left != null) root.left.infixForeach(op)
            op(root.value)
            if (root.right != null) root.right.infixForeach(op)
        }
    }
}

/**
 * 二叉搜索树节点
 * @tparam T
 */
class TreeNode[T: Ordering](var value: T) {

    //从冥界召唤Ordering[T]的隐式值当作比较器
    private val ord: Ordering[T] = implicitly[Ordering[T]]

    var left: TreeNode[T] = _
    var right: TreeNode[T] = _
    //父节点
    var parent: TreeNode[T] = _

    def add(ele: T): Unit= {
        //ele 小于当前节点则设为左子节点，否则设为右子节点
        if (ord.lteq(ele, value)) {
            //如果左节点为null，则直接设置为左节点，否则交给左节点继续处理
            if (left == null) {
                left = new TreeNode[T](ele)
                left.parent = this
            }
            else left.add(ele)
        } else {
            //如果右节点为null，则直接设置为右节点，否则交给右节点继续处理
            if (right == null) {
                right = new TreeNode[T](ele)
                right.parent = this
            }
            else right.add(ele)
        }
    }

    def delete(ele: T): Option[T]= {
        //1,当前节点即需要删除的
        //2,删除的是小于当前节点
        //3,删除的是大于当前节点
        if (ord.equiv(ele, value)) {
            var isLeft = true
            if (parent != null && this.eq(parent.right)) {
                isLeft = false
            }
            //判断删除的是否叶子节点
            if (this.left == null && this.right == null) {
                if (isLeft) parent.left = null
                else parent.right = null
            } else if (this.left != null && this.right != null) {
                this.value = this.right.delete(this.right.min).get
            } else {
                //要么有左节点，要么有右节点
                //先找到非空的子节点
                val nonNullChildNode = if (left != null) left else right
                nonNullChildNode.parent = parent
                if (isLeft) parent.left = nonNullChildNode
                else parent.right = nonNullChildNode
            }
            Some(ele)
        } else if (ord.lt(ele, value)) {
            if (left == null) None
            else left.delete(ele)
        } else {
            if (right == null) None
            else right.delete(ele)
        }
    }

    /**
     * 查找出最小的节点
     * @return
     */
    def min: T = {
        var minNode = this
        while (minNode.left != null) {
            minNode = minNode.left
        }
        minNode.value
    }


    def search(ele: T): TreeNode[T] = {
        //1,先判断当前节点是否要找的节点
        //2,不是，则与当前节点比较，看去左节点找还是右节点找
        if (ord.equiv(ele, value)) this
        else if (ord.lt(ele, value) && left != null) left.search(ele)
        else if (ord.gt(ele, value) && right != null) right.search(ele)
        else null
    }

    /**
     * 二叉搜索树使用中序遍历比较合适
     * @param op
     */
    def infixForeach(op: T => Unit): Unit = {
        if (left != null) left.infixForeach(op)
        op(value)
        if (right != null) right.infixForeach(op)
    }

    override def toString: String = {
        s"value = ${value}"
    }
}

