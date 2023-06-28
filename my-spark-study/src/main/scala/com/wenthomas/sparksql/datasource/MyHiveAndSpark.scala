package com.wenthomas.sparksql.datasource

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Verno
 * @create 2020-03-21 11:47 
 */
/**
 * 使用代码访问hive
 */
object MyHiveAndSpark {

    def main(args: Array[String]): Unit = {
        //设置系统环境变量：以atguigu身份访问hdfs
        System.setProperty("HADOOP_USER_NAME", "atguigu")

        val conf = new SparkConf().setAppName("MyHiveAndSpark").setMaster("local[*]")

        val spark = SparkSession
                .builder()
                .config(conf)
                //添加支持外置hive
                .enableHiveSupport()
                //配置hive数仓位置：默认为本地，所以要手动指向hdfs
                .config("spark.sql.warehouse.dir", "hdfs://mydata01:9000/user/hive/warehouse")
                .getOrCreate()

        import spark.implicits._
        //--------------------------------------------------------------------------------------------------------
        //(1) 读取Hive数据
        spark.sql("use sparkpractice")

        val df = spark.sql("select * from city_info")

        //--------------------------------------------------------------------------------------------------------
        //(2) 向Hive写入数据（实际数据写入到hdfs）
        /**
         * 注意：
         * 1）如果在spark中创建数据库，默认是在本地创建，需要在SparkSession中配置.config("spark.sql.warehouse.dir", "hdfs://mydata01:9000/user/hive/warehouse")
         * 2）df如果做了聚合，默认会产生200个分区，可在write前手动合并分区减少数据量
         */
        spark.sql("create database sparklearning")
        spark.sql("use sparklearning")

        //      方式一：使用Hive的insert into语句，前提是表必须存在
        spark.sql("create table user1(id int, name string)").show
        spark.sql("insert into user1 values(10, 'lisi')").show


        //      方式二：df.write.saveAsTable（“表名”）将数据保存在hive表中，如果表不存在会自动创建
        df.write.mode("append").saveAsTable("user2")


        //      方式三：df.write.insertInto("表名")   --基本等价于df.write.mode("append").saveAsTable("表名")
        df.write.insertInto("user2")  // 基本等于 mode("append").saveAsTable("user2")


        spark.close()
    }

}
