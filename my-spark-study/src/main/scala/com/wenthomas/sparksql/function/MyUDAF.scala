package com.wenthomas.sparksql.function

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType, StructField, StructType}

/**
 * @author Verno
 * @create 2020-03-20 15:20 
 */
/**
 * 自定义聚合函数(弱类型)：
 *      继承UserDefinedAggregateFunction抽象类
 * 注意：只能用在sql语句中，无法在dsl语句中使用
 *
 *
 */
object MyUDAF {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyUDAF").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val df = spark.read.json("file:///E:/1015/my_data/1.json")

        import spark.implicits._
        //--------------------------------------------------------------------------------------------------------
        df.createOrReplaceTempView("user1")
        //查看df的元数据信息
        df.printSchema()

        //通过Spark注册自定义UDAF函数
        val avg = new MyAvg
        spark.udf.register("myAvg", avg)

        //测试使用自定义函数
        spark.sql(
            """
                |select myAvg(age) avg_name from user1
                |""".stripMargin
        ).show

        spark.stop()
    }

}

/**
 * 自定义UDAF：继承UserDefinedAggregateFunction抽象类
 */
class MyAvg extends UserDefinedAggregateFunction {
    //输入的数据类型
    override def inputSchema: StructType = {
        //注意：这里有多少个StructField，使用该自定义函数时就要传入相应的参数，否则报错
//        StructType(StructField("name", StringType)::StructField("age", LongType)::Nil)
        StructType(StructField("age", LongType)::Nil)
    }

    //缓冲区的类型：用于保存迭代过程值
    override def bufferSchema: StructType = {
        StructType(StructField("total", LongType)::StructField("count", LongType)::Nil)
    }

    //聚合结果的数据类型
    override def dataType: DataType = {
        DoubleType
    }

    //相同的输入是否返回相同的输出：与幂等性相关，一般返回true
    override def deterministic: Boolean = true

    //对缓冲区进行初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        //等价于buffer.upadte(0, 0L)
        buffer(0) = 0L
        buffer(1) = 0L
    }

    //分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        //input是指的是使用聚合函数的时候，传过来的参数封装到了Row
        //注意：加入判断，考虑到传入的字段可能是null
        if (!input.isNullAt(0)) {
            //这里input的参数索引要与inputSchema方法中定义的一致
            buffer(0) = buffer.getLong(0) + input.getAs[Long](0)
            buffer(1) = buffer.getLong(1) + 1L
        }
    }

    //分区间合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        //把buffer1和buffer2的缓冲居合道一起，然后再把值写回到buffer1
        //注意：不需要非空判断，因为缓冲区初始化时一定会给一个默认值
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //返回聚合结果
    override def evaluate(buffer: Row): Any = {
        //total/count求平均值返回
        buffer.getLong(0).toDouble / buffer.getLong(1)
    }
}
