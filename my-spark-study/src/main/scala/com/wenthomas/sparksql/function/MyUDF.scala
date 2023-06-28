package com.wenthomas.sparksql.function

import com.wenthomas.sparksql.User
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Verno
 * @create 2020-03-20 15:14 
 */

/**
 * 自定义UDF函数：
 * 通过spark注册自定义UDF函数
 * spark.udf.register("函数名", func）
 */
object MyUDF {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyUDF").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val rdd = spark.sparkContext.makeRDD(Array(User("zhangsan",10),User("lisi",20),User("wangwu",15)))

        import spark.implicits._
        //--------------------------------------------------------------------------------------------------------
        val df = rdd.toDF

        df.createOrReplaceTempView("user")
        //通过spark注册自定义UDF函数
        spark.udf.register("myUDF", (s:String) => s.toUpperCase)

        //测试使用自定义函数
        spark.sql(
            """
                |select myUDF(name) name from user
                |""".stripMargin
        ).show




        spark.stop()
    }

}
