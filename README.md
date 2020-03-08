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