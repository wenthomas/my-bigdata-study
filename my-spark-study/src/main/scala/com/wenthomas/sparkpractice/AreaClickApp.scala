package com.wenthomas.sparkpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Verno
 * @create 2020-03-21 3:08 
 */
object AreaClickApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("AreaClickApp").setMaster("local[*]")

        //添加hive支持
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

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
                |	city_remark(city_name)
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

        spark.sql(
            """
                |select
                |	*
                |from
                |	tb3
                |where rank <= 3
                |""".stripMargin).show()



        spark.stop()
    }

}
