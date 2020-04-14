package com.wenthomas.alogorithms.tree

/**
 * @author Verno
 * @create 2020-04-13 16:32 
 */
/**
 * 平衡二叉树----自定义AVL树
 */
object AVLTreeDemo {
    def main(args: Array[String]): Unit = {
        val arr = Array(5, 8, 3, 2, 6, 1, 10, 40, 23, 15, 37)
        val tree = new AVLTree[Int]

        arr.foreach(x => tree.add(x))

        /*        tree.infixForeach(println)
                println(tree.search(65))*/
        tree.delete(1335)
        tree.infixForeach(println)
        println(tree.search(123).getHeight)
        //todo: 测试
    }
}

/**
 * AVL树
 * @tparam T
 */
class AVLTree[T: Ordering] {
    //从冥界召唤Ordering[T]的隐式值当作比较器
    private val ord: Ordering[T] = implicitly[Ordering[T]]

    var root: AVLTreeNode[T] = _

    def add(ele: T) = {
        //如果是第一次添加，自动设置为根节点
        if (root == null) root = new AVLTreeNode[T](ele)
        else {
            root.add(ele)
            if (root.parent != null) {
                //todo: 平衡根节点
            }
        }
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

    def search(ele: T): AVLTreeNode[T] = {
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

    /**
     * 获取树的高度
     */
    def getHeight = {

    }
}

/**
 * AVL树节点
 * @tparam T
 */
class AVLTreeNode[T: Ordering](var value: T) {

    //从冥界召唤Ordering[T]的隐式值当作比较器
    private val ord: Ordering[T] = implicitly[Ordering[T]]

    var left: AVLTreeNode[T] = _
    var right: AVLTreeNode[T] = _
    //父节点
    var parent: AVLTreeNode[T] = _

    def add(ele: T): Unit= {
        //ele 小于当前节点则设为左子节点，否则设为右子节点
        if (ord.lteq(ele, value)) {
            //如果左节点为null，则直接设置为左节点，否则交给左节点继续处理
            if (left == null) {
                left = new AVLTreeNode[T](ele)
                left.parent = this
            }
            else left.add(ele)
        } else {
            //如果右节点为null，则直接设置为右节点，否则交给右节点继续处理
            if (right == null) {
                right = new AVLTreeNode[T](ele)
                right.parent = this
            }
            else right.add(ele)
        }
        //todo： 旋转

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
     * 旋转
     * @return
     */
    def rotate(): Unit = {
        //左左 -> 右旋
        if (getLeftHeight - getRightHeight > 1 && left.getLeftHeight - left.getRightHeight > 0) {
            rightRotate
            return
        }
        //右右 -> 左旋
        if (getLeftHeight - getRightHeight < -1 && right.getLeftHeight - right.getRightHeight < 0) {
            leftRotate
            return
        }
        //左右 -> 左旋 然后 右旋
        if (getLeftHeight - getRightHeight > 1 && left.getLeftHeight - left.getRightHeight < 0) {
            //当前节点左边高，当前节点的左节点是右边高
            //对当前失衡节点的左节点做左旋
            left.leftRotate
            //当前节点做右旋
            rightRotate
            return
        }
        //右左 -> 右旋 然后 左旋
        if (getLeftHeight - getRightHeight < -1 && right.getLeftHeight - right.getRightHeight > 0) {
            //对当前失衡的右节点做右旋
            right.rightRotate
            //对当前节点做左旋
            leftRotate
            return
        }
    }

    /**
     * 左旋
     */
    def leftRotate = {
        //1，先缓存需要变化的节点
        val tmpParent = parent
        val tmpRight = right
        val tmpRightLeft = right.left
        //2，旋转操作
        //让当前节点的右节点指向原来右节点的左节点
        right = tmpRightLeft
        //让原来的右节点的左节点指向当前节点
        tmpRight.left = this
        //让父节点的左节点指向原来的右节点
        if (tmpParent != null && tmpParent.left == this) {
            //当前节点是其父节点的左节点
            tmpParent.left = tmpRight
        } else if (tmpParent != null) {
            tmpParent.right = tmpRight
        }
        //建立父节点关系
        if (tmpRightLeft != null) tmpRightLeft.parent = this
        this.parent = tmpRight
        tmpRight.parent = parent

/*        parent = tmpParent
        right.left.parent = this
        parent.left = tmpRight*/
    }

    /**
     * 右旋
     */
    def rightRotate = {
        //1，先缓存需要变化的节点
        val tmpParent = parent
        val tmpLeft = left
        //有可能为空
        val tmpLeftRight = left.right

        //2，旋转操作
        //让当前节点成为左节点的右节点
        left = tmpLeftRight
        //让左节点的右节点指向当前节点
        tmpLeft.right = this
        //当前节点的父节点的左（右）节点指向当前节点的左节点
        //todo: 理解
        if (tmpParent != null && tmpParent.left == this) {
            tmpParent.left = tmpLeft
        } else if (tmpParent != null) {
            tmpParent.right = tmpLeft
        }
        //更新父节点
        if (tmpLeftRight != null) tmpLeftRight.parent = this
        this.parent = tmpLeft
        tmpLeft.parent = tmpParent
    }

    /**
     * 获取树节点的高度
     */
    def getHeight: Int = {
        //比较左右子树，获取最高高度，然后+1即为当前树的高度
        getLeftHeight.max(getRightHeight) + 1
    }

    /**
     * 获取左子树的高度
     */
    def getLeftHeight = {
        if (left == null) -1 else left.getHeight
    }

    /**
     * 获取右子树的高度
     */
    def getRightHeight = {
        if (right == null) -1 else right.getHeight
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


    def search(ele: T): AVLTreeNode[T] = {
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