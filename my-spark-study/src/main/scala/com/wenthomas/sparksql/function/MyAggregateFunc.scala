package com.wenthomas.sparksql.function

import com.wenthomas.sparksql.User1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author Verno
 * @create 2020-03-20 16:22 
 */

/**
 * 自定义聚合函数(强类型)：
 *      继承UserDefinedAggregateFunction抽象类
 * 注意：只能用在sql语句中，无法在dsl语句中使用
 *
 *
 */
object MyAggregateFunc {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyUDAF").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val df = spark.read.json("file:///E:/1015/my_data/1.json")

        import spark.implicits._
        //--------------------------------------------------------------------------------------------------------
        val ds = df.as[User2]

        //强类型聚合函数的注册使用
        val myfunc = new MyAggAvg().toColumn.name("myFunc")

        val result = ds.select(myfunc)
        result.show()




        spark.stop()
    }

}




case class User2(name: String, age: Long)

/**
 * 自定义缓存类：用来存储MyAggAvg聚合的缓冲区数据
 * 注意：由于样例类属性默认val，为了迭代需要，属性需要手动设定为var，或者在聚合时赋值给新对象
 * @param total
 * @param count
 */
case class AvgBuffer(total: Long, count: Long)

/**
 * 自定义聚合函数(强类型)：继承UserDefinedAggregateFunction抽象类
 */
class MyAggAvg extends Aggregator[User2, AvgBuffer, Double] {
    //对缓冲区进行初始化
    override def zero: AvgBuffer = AvgBuffer(0L, 0L)

    //聚合（分区内聚合）
    override def reduce(b: AvgBuffer, a: User2): AvgBuffer = {
        //注意：需要对输入进行非空判断
        a match {
                //如果是User2类型，则进行赋值迭代
            case User2(name, age) => AvgBuffer(b.total + age, b.count + 1)
                //如果是null，则原封不动返回
            case _ => b
        }
    }

    //分区间聚合
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
        AvgBuffer(b1.total + b2.total, b1.count + b2.count)
    }

    //返回聚合结果
    override def finish(reduction: AvgBuffer): Double = {
        reduction.total.toDouble / reduction.count
    }

    //对缓冲区进行编码
    override def bufferEncoder: Encoder[AvgBuffer] = {
        //如果是样例类，就直接返回这个编码器即可，（因为所以元组和样例类都继承了Product）
        Encoders.product
    }

    //对返回值进行编码
    override def outputEncoder: Encoder[Double] = {
        Encoders.scalaDouble
    }
}