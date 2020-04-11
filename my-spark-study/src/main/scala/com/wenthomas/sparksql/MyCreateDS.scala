package com.wenthomas.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Verno
 * @create 2020-03-20 14:09 
 */
/**
 * 创建DataSet
 */
object MyCreateDS {

    def main(args: Array[String]): Unit = {
        //1，创建SparkSession
        val conf = new SparkConf().setAppName("MyCreateDS").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()
        //--------------------------------------------------------------------------------------------------------
        //(1)，通过集合创建DF(需要List，不能用Array)
        /**
         * 注意：需要导入spark对象中的implicits对象中的所有方法（隐式转换）
         */
        import spark.implicits._
        val list = (1 to 20).toList
        val ds = list.toDS

        /**
         * 注意：DF能用的DS一定可以用
         */
        ds.show()
        //--------------------------------------------------------------------------------------------------------
        //(2)通过样例类User反射转换
        val list1 = Array(User("zhangsan", 10), User("lisi", 20), User("wangwu", 15)).toList
        val ds1 = list1.toDS

        //在ds做sql查询
        ds1.createOrReplaceTempView("user")
        spark.sql(
            """
                |select * from user
                |""".stripMargin
        ).show()

        //4，关闭SparkSession
        spark.close()
    }

}
