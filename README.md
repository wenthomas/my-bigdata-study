# my-bigdata-study

大数据学习练习

## 一,Flume
### 1，自定义Source
[案例](src/main/java/com/wenthomas/flume/source/MySource.java)<br/>
[配置文件](src/main/resources/com/wenthomas/flume/source/)<br/>
### 2，自定义Interceptor
[案例](src/main/java/com/wenthomas/flume/interceptor/MyInterceptor.java)<br/>
[配置文件](src/main/resources/com/wenthomas/flume/interceptor/)<br/>
### 1，自定义Source
[案例](src/main/java/com/wenthomas/flume/sink/MySink.java)<br/>
[配置文件](src/main/resources/com/wenthomas/flume/sink/)<br/>

## 三,Hbase
引入依赖：
```xml
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-server</artifactId>
            <version>1.3.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>1.3.1</version>
        </dependency>

        <dependency>
            <groupId>jdk.tools</groupId>
            <artifactId>jdk.tools</artifactId>
            <version>1.8</version>
            <scope>system</scope>
            <systemPath>${JAVA_HOME}/lib/tools.jar</systemPath>
        </dependency>
```
备注：<br/>
在对HBase执行增删改查时，只需要引入hbase-client模块即可，运行MR操作hbase时，需要引入hbase-server。
拷贝hdfs-site.xml文件到客户端的类路径下！<br/>
hbase-site.xml
[配置文件](my-hbase-study/src/main/resources/hbase-site.xml)<br/>

### 0，获取Connection连接
ConnectionUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/ConnectionUtil.java)<br/>
### 1，数据库操作
NameSpaceUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/NameSpaceUtil.java)<br/>
### 2，数据库表操作
TableUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/TableUtil.java)<br/>
### 3，数据操作
DataUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/DataUtil.java)<br/>

## 五,Scala
### 1，HelloWorld
[案例](my-scala-study/src/main/scala/com/wenthomas/helloworld/HelloWorld.scala)<br/>

### 2，循环流程结构
判断质数函数（for循环判断）
[案例](my-scala-study/src/main/scala/com/wenthomas/loop/PrimeNum.scala)<br/>

### 3，递归与尾递归
[案例](my-scala-study/src/main/scala/com/wenthomas/loop/TailLoop.scala)<br/>

### 补充：高阶函数

### 4，面向对象
#### 4.1，面向对象
继承案例[案例](my-scala-study/src/main/scala/com/wenthomas/bank/Bank.scala)<br/>
抽象类案例[案例](my-scala-study/src/main/scala/com/wenthomas/abs/CaculateArea.scala)<br/>
枚举类案例[案例](my-scala-study/src/main/scala/com/wenthomas/poke/Poke.scala)<br/>
#### 4.1，特质
[案例](my-scala-study/src/main/scala/com/wenthomas/myTrait/MyTrait.scala)<br/>
#### 4.3，单例对象（伴生对象与伴生类）
[案例](my-scala-study/src/main/scala/com/wenthomas/single/Point.scala)<br/>

### 5，隐式转换
[案例](my-scala-study/src/main/scala/com/wenthomas/myImplicit/MyImplicit.scala)<br/>

### 6，集合
#### 6.1，数组
[案例](my-scala-study/src/main/scala/com/wenthomas/collections/Collect.scala)<br/>
#### 6.2，高级函数
map()和foreach():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/ForeachAndMap.scala)<br/>
Iterator迭代器:[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/MyIterator.scala)<br/>
reduce():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/MyReduce.scala)<br/>
折叠foldLeft():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/FoldAndReduce.scala)<br/>
scanLeft():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/MyScanLeft.scala)<br/>
滑窗sliding():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/MySlide.scala)<br/>
拉链zip():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/MyZip.scala)<br/>
分组groupBy():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/MyGroupBy.scala)<br/>
排序sorted()、sortBy()、sortWith():[案例](my-scala-study/src/main/scala/com/wenthomas/collections/MySort.scala)<br/>
Stream数据流:[案例](my-scala-study/src/main/scala/com/wenthomas/collections/MyStream.scala)<br/>
并行集合:[案例](my-scala-study/src/main/scala/com/wenthomas/collections/ParSeq.scala)<br/>
#### 6.3，WordCount字符统计
案例一[案例](my-scala-study/src/main/scala/com/wenthomas/collections/wordCount/MyCount1.scala)<br/>
案例二[案例](my-scala-study/src/main/scala/com/wenthomas/collections/wordCount/MyCount2.scala)<br/>
#### 6.4，应用案例
6.4.1,map()映射应用[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/Practice.scala)<br/>
6.4.2,合并两个Map集合[案例](my-scala-study/src/main/scala/com/wenthomas/collections/highFunctions/CombineMaps.scala)<br/>
6.4.3,MapIndexes[案例](my-scala-study/src/main/scala/com/wenthomas/collections/homework/MapIndexes.scala)<br/>
6.4.4,通过foldLeft折叠来模拟mkString方法[案例](my-scala-study/src/main/scala/com/wenthomas/collections/homework/MyMkString.scala)<br/>
6.4.5,通过foldLeft折叠来模拟reverse反转方法[案例](my-scala-study/src/main/scala/com/wenthomas/collections/homework/MyReverse.scala)<br/>
6.4.6,通过reduce聚合来求集合中的最大值[案例](my-scala-study/src/main/scala/com/wenthomas/collections/homework/GetMaxByReduce.scala)<br/>
6.4.7,通过foldLeft折叠来同时求集合中的最大值和最小值[案例](my-scala-study/src/main/scala/com/wenthomas/collections/homework/GetMaxAndMinByFoldLeft.scala)<br/>
### 7, 模式匹配
#### 7.1，应用案例
（1）. 利用模式匹配，编写一个 swap(arr: Array[Int]) 函数，交换数组中前两个元素的位置<br/>
[答案](my-scala-study/src/main/scala/com/wenthomas/pattern/homework/MySwap.scala)<br/>
```scala
package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 0:15 
 */
/**
 * 利用模式匹配，编写一个 swap(arr: Array[Int]) 函数，交换数组中前两个元素的位置
 */
object MySwap extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)

    def mySwapFirstAndSecond(arr: Array[Int]): Array[Int] = {
        val result = arr match {
            case Array(a, b, _*) => {
                arr(0) = b
                arr(1) = a
                arr
            }
        }
        result
    }

    println(mySwapFirstAndSecond(list1.toArray).mkString(","))
}
```
（2）. 编写一个函数，计算 List[Option[Int]] 中所有非None值之和。分别使用 match 和不适用 match 来计算<br/>
[答案](my-scala-study/src/main/scala/com/wenthomas/pattern/homework/MySumInt.scala)<br/>
```scala
package com.wenthomas.pattern.homework

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author Verno
 * @create 2020-03-10 0:47 
 */
/**
 * 编写一个函数，计算 List[Option[Int]] 中所有非None值之和。分别使用 match 和不适用 match 来计算
 */
object MySumInt extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)

    //List(Some(30), None, None, None, Some(10), Some(20))
    private val options: List[Option[Int]] = list1.map(x => if (x < 40) Some(x) else None)

    //方法一：match模式匹配实现
    def mySumInt1(list: List[Option[Int]]): Int = {
        list.foldLeft(0)((sum, x) => {
            x.isEmpty match {
                case true =>
                    sum
                case false =>
                    sum + x.get
            }})
    }

    println(mySumInt1(options))

    //方法二：一般方法实现
    def mySumInt2(list: List[Option[Int]]): Int = {
        list.foldLeft(0)((sum, x) => {
            if (x.isEmpty) sum else sum + x.get
        })
    }

    println(mySumInt2(options))
}
```
（3）. 我们可以用列表制作只在叶子节点存放值的树。举例来说，列表((3 8) 2 (5)) 描述的是如下这样一棵树：
    List[Any] = List(List(3, 8), 2, List(5))
```text
        *
       /|\
      * 2 *
     /\   |
    3  8  5
```
   
不过，有些列表元素是数字，而另一些是列表。在Scala中，你必须使用List[Any]。<br/>
编写一个leafSum函数，计算所有叶子节点中的元素之和.<br/>
[答案](my-scala-study/src/main/scala/com/wenthomas/pattern/homework/MySumLeaf.scala)<br/>
```scala
package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 1:22 
 */
/**
 * 我们可以用列表制作只在叶子节点存放值的树。举例来说，列表((3 8) 2 (5)) 描述的是如下这样一棵树：
 *          *
 *         /|\
 *        * 2 *
 *       /\   |
 *      3  8  5
 * List[Any] = List(List(3, 8), 2, List(5))
 * 不过，有些列表元素是数字，而另一些是列表。在Scala中，你必须使用List[Any]。
 * 编写一个leafSum函数，计算所有叶子节点中的元素之和.
 */
object MySumLeaf extends App {

    val list:List[Any] = List(List(3, 8), 2, List(5))

    //使用递归完成
    def leafSum(list: List[Any]): Int = {
        var sum = 0
        list.foreach(x => {
            x match {
                case list:List[_] => sum += leafSum(list)
                case i:Int => sum += i
            }
        })
        sum
    }

    println(leafSum(list))

}
```
（4）.  如果使用样例会更好一些, 从二叉树开始<br/>
```scala
    sealed abstract class BinaryTree
    case class Leaf(value : Int) extends BinaryTree
    case class Node(left: BinaryTree, right: BinaryTree) extends BinaryTree
```

编写一个函数计算所有叶子节点的元素的和：
val r = Node(Leaf(8), Node(Leaf(3), Leaf(9))) <br/>
[答案](my-scala-study/src/main/scala/com/wenthomas/pattern/homework/MySumLeaf2.scala)<br/>
```scala
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
            }
            sum
        }
        loop(tree)
    }

    println(mySumLeaf3(r))
}
```
（5）. 附加题(认为自己够牛逼的可以完成下): <br/>
大公司面试题: 使用递归<br/>
假设某国的货币有若干面值，现给一张大面值的货币要兑换成零钱，问有多少种兑换方式。<br/>
[答案](my-scala-study/src/main/scala/com/wenthomas/pattern/homework/MyCashExchange.scala)<br/>
```scala
package com.wenthomas.pattern.homework

/**
 * @author Verno
 * @create 2020-03-10 2:11 
 */
/**
 * 附加题(认为自己够牛逼的可以完成下):
 * 大公司面试题: 使用递归
 * 假设某国的货币有若干面值，现给一张大面值的货币要兑换成零钱，问有多少种兑换方式
 */
object MyCashExchange extends App {

    def myCashExchange(cash: Int, coins: List[Int]): Int = {
        val tuple = (cash, coins)
        tuple match {
            case (a, b) if a == 0 =>
                1
            case (a, b) if a <0 || b.size == 0 =>
                0
            case (a, b) =>
                myCashExchange(a, b.tail) + myCashExchange(a - b.head, b)
        }
    }

    val coins = List(1, 5, 10, 20, 50, 100);
    println(myCashExchange(100, coins))
}
```

