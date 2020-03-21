package com.wenthomas.sparkpractice

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author Verno
 * @create 2020-03-21 3:08 
 */

/**
 * 注意：在服务器中需要先打开 thriftserver 以提供代码访问hive的接口
 */
object AreaClickApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("AreaClickApp").setMaster("local[*]")

        //添加外置hive支持
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
        //---------------------------------------------------------------------------------------------------
        import spark.implicits._

        //注册自定义聚合函数
        val myUDAF = AreaClickUDAF
        spark.udf.register("city_remark", myUDAF)

        //1,指定数据库
        spark.sql(
            """
                |use sparkpractice
                |""".stripMargin)

        //2, 查询数据
        spark.sql(
            """
                |select
                |	uva.click_product_id ,
                |	pi.product_name,
                |	ci.city_name,
                |	ci.area
                |from
                |	user_visit_action uva
                |join product_info pi on
                |	uva.click_product_id = pi.product_id
                |join city_info ci on
                |	uva.city_id = ci.city_id
                |where click_product_id > -1
                |""".stripMargin).createOrReplaceTempView("tb1")


        spark.sql(
            """
                |select
                |	area,
                |	product_name,
                |	COUNT(*) click_count,
                |	city_remark(city_name) `city_remark`
                |from
                |	tb1
                |group by
                |	area,
                |	product_name
                |""".stripMargin).createOrReplaceTempView("tb2")

        spark.sql(
            """
                |select
                |	*,
                |	rank() OVER(PARTITION by area order by click_count desc) rank
                |from
                |	tb2
                |""".stripMargin).createOrReplaceTempView("tb3")

        val resultDF = spark.sql(
            """
                |select
                |	area,
                |   product_name,
                |   click_count,
                |   `city_remark`
                |from
                |	tb3
                |where tb3.rank <= 3
                |""".stripMargin)

        resultDF.show()

        //---------------------------------------------------------------------------------------------------
        //将查询结果写入到mysql中

        val props = new Properties()
        props.put("user", "root")
        props.put("password", "root")

        //由于df默认生成200分区，需要.coalesce手动合并分区以减少文件数量
        resultDF
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:mysql://localhost:3306/spark?useUnicode=true&characterEncoding=utf8", "user_action_area_collect", props)



        spark.close()
    }

}
