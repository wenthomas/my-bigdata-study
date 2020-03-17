package com.wenthomas.sparkcore.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Verno
 * @create 2020-03-17 11:10 
 */
object MyHbaseRead extends App {

    private val conf: SparkConf = new SparkConf().setAppName("MyDBCRead").setMaster("local[*]")
    private val sc = new SparkContext(conf)

    private val conn: Configuration = HBaseConfiguration.create()
    conn.set("hbase.zookeeper.quorum", "mydata01,mydata02,mydata03")
    conn.set(TableInputFormat.INPUT_TABLE, "s")

}
