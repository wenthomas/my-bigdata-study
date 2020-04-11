package com.wenthomas.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author Verno
 * @create 2020-03-20 11:35 
 */

/**
 * RDD到DF的转换
 * (1) rdd.toDF
 * (2) 通过样例类User反射转换(最常用)
 * (3) 直接从scala集合通过toDF转到df，一般用来测试用（不能用Array）
 * (4) 通过Spark API 的方式转换(了解)
 */
object MyRDD2DF {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyRDD2DF").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val rdd = spark.sparkContext.makeRDD(1 to 10)
        val rdd1 = spark.sparkContext.makeRDD(Array(("zhangsan",10),("lisi",20),("wangwu",15)))
        //--------------------------------------------------------------------------------------------------------
        //RDD 到 DF的转换
        /**
         * 注意：需要导入spark对象中的implicits对象中的所有方法（隐式转换）
         */
        import spark.implicits._
        //--------------------------------------------------------------------------------------------------------
        //(1) rdd.toDF
        val df = rdd.toDF

        //          toDF中可传入列名，参数顺序与数据的列对应
        val df1 = rdd1.toDF("name", "age")

        //--------------------------------------------------------------------------------------------------------
        //(2) 通过样例类User反射转换(最常用)
        val rdd2 = spark.sparkContext.makeRDD(Array(User("zhangsan",10),User("lisi",20),User("wangwu",15)))
        //          通过样例类的话不需要再指定列名
        /**
         * +--------+---+
         * |    name|age|
         * +--------+---+
         * |zhangsan| 10|
         * |    lisi| 20|
         * |  wangwu| 15|
         * +--------+---+
         */
        val df2 = rdd2.toDF
        //--------------------------------------------------------------------------------------------------------
        //(3)直接从scala集合通过toDF转到df，一般用来测试用（不能用Array）
        val df3 = (1 to 20).toDF("number")
        df3.show()
        //      DF转RDD
        val rdd3 = df.rdd
        rdd3.foreach(println)
        rdd3.map(row => row.getInt(0)).foreach(println)

        //--------------------------------------------------------------------------------------------------------
        //(4)通过Spark API 的方式转换(了解)
        val rdd4 = spark.sparkContext.makeRDD(("lisi", 10) :: ("zhangsan", 20) :: Nil)
        //      rdd需要先转成Row集合
        val rdd5 = rdd4.map({
            case (name, age) => Row(name, age)
        })
        //      给Row中的数据定义数据类型
        val schema = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
        val df5 = spark.createDataFrame(rdd5, schema)
        df5.show()




        //4，关闭SparkSession
        spark.close()
    }

}
case class User(name: String, age: Int)
