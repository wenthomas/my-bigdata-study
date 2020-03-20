package com.wenthomas.sparksql.datasource

import com.wenthomas.sparksql.function.User2
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author Verno
 * @create 2020-03-20 16:48 
 */

/**
 * 通用读写数据源方法：
 *      通用读：spark.read.format("文件格式类型").load(文件路径)
 *      通用写：df.write.mode("文件保存模式").format("文件输出格式").save(输出路径)
 */
object MyCommonReadAndWrite {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyCommonReadAndWrite").setMaster("local[*]")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._
        //--------------------------------------------------------------------------------------------------------
        //(1) 通用读写json文件
        //      读
        // spark.read.format("json").load(文件路径) 或者 spark.read.json(文件路径)
        val df = spark.read.format("json").load("file:///E:/1015/my_data/1.json")

        val df1 = spark.read.json("file:///E:/1015/my_data/1.json")

        //      直接在json文件上执行SQL查询
        spark.sql(
            """
                |select * from json.`file:///E:/1015/my_data/1.json`
                |""".stripMargin).show()

        //      写
//        df1.write.mode("append").format("json").save("output/")
//        df1.write.mode("append").json("output/")


        //--------------------------------------------------------------------------------------------------------
        //(2) 通用读parquet文件（默认）
        //      读
/*        val parDF = spark.read.parquet("output/user.parquet/part-00000-57dca19e-6cfc-4b2f-844f-7971b7dce51d.snappy.parquet")
        val userDS = parDF.as[User2]
*/


        //      写
        //df.write.mode(SaveMode.Overwrite).parquet("output/user.parquet")


        spark.stop()
    }

}
