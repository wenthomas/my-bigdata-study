package com.wenthomas.sparkcore.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/**
 * @author Verno
 * @create 2020-03-17 14:12 
 */

/**
 * RDD从Hbase中读写数据：
 *  实际上是借用了Hbase集成MR的能力，使用MR方式运算能力。
 *  由于 org.apache.hadoop.hbase.mapreduce.TableInputFormat 类的实现，Spark 可以通过Hadoop输入格式访问 HBase。
 *
 *  这个输入格式会返回键值对数据，其中键的类型为org. apache.hadoop.hbase.io.ImmutableBytesWritable，
 *  而值的类型为org.apache.hadoop.hbase.client.Result
 */
object MyHbaseWrite {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyDBCRead").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val conn: Configuration = HBaseConfiguration.create()
        conn.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
        conn.set(TableOutputFormat.OUTPUT_TABLE, "student")

        //通过job来设置输出的格式的类
        val job = Job.getInstance(conn)

        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])

        val rdd = sc.makeRDD(List(("1003", "steven", "10"), ("1004", "verno", "20")))

        // 先把RDD封装成TableReduce需要的格式
        val hbaseRDD = rdd.map({
            case (rk, name, age) => {
                val rowkey = new ImmutableBytesWritable()
                rowkey.set(Bytes.toBytes(rk))
                val put = new Put(Bytes.toBytes(rk))
                // 设置列族及列值
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age))
                (rowkey, put)
            }
        })
        hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

        sc.stop()
    }

}

