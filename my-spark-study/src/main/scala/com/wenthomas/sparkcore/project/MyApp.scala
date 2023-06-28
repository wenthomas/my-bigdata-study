package com.wenthomas.sparkcore.project

import com.wenthomas.sparkcore.project.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-19 15:58 
 */
object MyApp {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        val sc = new SparkContext(conf)

        //加载文件
        val path = "file:///E:/1015/1015课堂资料/502_大数据之 spark/02_资料/user_visit_action.txt"
        val lineRDD = sc.textFile(path)

        //将数据封装入UserVisitAction样例类中
        //封装前需要ETL
        val userVisitActionRDD = lineRDD
                .map(line => line.split("_"))
                .filter(arr => arr.length == 13).map(fields =>
            UserVisitAction(
                fields(0),
                fields(1).toLong,
                fields(2),
                fields(3).toLong,
                fields(4),
                fields(5),
                fields(6).toLong,
                fields(7).toLong,
                fields(8),
                fields(9),
                fields(10),
                fields(11),
                fields(12).toLong))

        //需求一：Top10 热门品类
        val categoryTop10 = MyReq.request1(sc, userVisitActionRDD)

        //需求2: top10品类的top10session
        //MyReq.statCategorySessionTop10_1(sc, categoryTop10, userVisitActionRDD)
//        MyReq.statCategorySessionTop10_2(sc, categoryTop10, userVisitActionRDD)
//        MyReq.statCategorySessionTop10_3(sc, categoryTop10, userVisitActionRDD)
        MyReq.statCategorySessionTop10_4(sc, categoryTop10, userVisitActionRDD)

        //需求三：页面单跳转化率统计
        MyReq.statPageConversionRate(sc, userVisitActionRDD, "1,2,3,4,5,6,7")




        sc.stop()

    }
}
