package com.wenthomas.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Verno
 * @create 2020-03-13 15:42 
 */
class Person(val name: String, val age: Int) extends Serializable {
    override def toString: String = s"{$name, $age}"
}

object MySortBy extends App {


    //1，得到SparkContext
    private val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    //2，创建RDD
    val arr = Array(7,6,8,4,7,5,2,9,1)
    private val rdd= sc.makeRDD(arr,3)

    private val rdd1: RDD[Person] = sc.makeRDD(
        new Person("wenthomas", 10) :: new Person("verno", 20) :: new Person("Tom", 15) :: Nil)

    //3，转换
    //sortBy(f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length):排序
    //排序的过程中调用了shuffle
    /**
     * 参数：
     * f: (T) => K : 比较标准
     * ascending：true升序，false降序
     * numPartitions： 分区数，默认与原分区数保持一致
     */
    private val sortRDD: RDD[Int] = rdd.sortBy(x => x, ascending = true)



    //比较自定义对象：如果是样例类，则无需任何处理，因为样例类自动实现了equals和hashcode方法，所以属性相同则对象相同
    //如果不是样例类，则需要实现一个隐式参数提供比较器
    implicit val ord = new Ordering[Person] {
        //根据年龄排序
        override def compare(x: Person, y: Person): Int = x.age - y.age
    }
    private val sortRDD1: RDD[Person] = rdd1.sortBy(x => x)







    //4，行动算子
    private val result = sortRDD.collect
    private val result1 = sortRDD1.collect

    // 1,2,4,5,6,7,7,8,9
    println(result.mkString(","))
    //{wenthomas, 10},{Tom, 15},{verno, 20}
    println(result1.mkString(","))

    //5，关闭连接
    sc.stop()

}
