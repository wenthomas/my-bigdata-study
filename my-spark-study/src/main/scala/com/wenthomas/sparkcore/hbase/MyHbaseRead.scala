package com.wenthomas.sparkcore.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Serialization

import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-17 11:10 
 */
/**
 * RDD从Hbase中读写数据：
 *  实际上是借用了Hbase集成MR的能力，使用MR方式运算能力。
 *  由于 org.apache.hadoop.hbase.mapreduce.TableInputFormat 类的实现，Spark 可以通过Hadoop输入格式访问 HBase。
 *
 *  这个输入格式会返回键值对数据，其中键的类型为org. apache.hadoop.hbase.io.ImmutableBytesWritable，
 *  而值的类型为org.apache.hadoop.hbase.client.Result
 */
object MyHbaseRead {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyDBCRead").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val conn: Configuration = HBaseConfiguration.create()
        conn.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
        conn.set(TableInputFormat.INPUT_TABLE, "student")

        //创建newAPIHadoopRDD从hbase读数据
        val rdd = sc.newAPIHadoopRDD(
            conn,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )

        val rdd1 = rdd.map({
                    //result.getRow：获取rowkey集合
            case (_, result: Result) => Bytes.toString(result.getRow)
        })


        //1001
        //1002
        rdd1.collect.foreach(println)

        //读取数据再封装
        val resultRDD = rdd.map({
            //iw只封装rowkey  result封装一行数据
            case (iw, result: Result) => {
                val map = mutable.Map[String, Any]()
                //把rowkey封装到map中
                map += "rowKey" -> Bytes.toString(iw.get())
                //再把每一列也存入到map中
                val cells = result.listCells()
                //引入scala的for语句遍历
                import scala.collection.JavaConversions._
                for (cell <- cells) {
                    //列名 -> 列值
                    val key = Bytes.toString(CellUtil.cloneQualifier(cell))
                    val value = Bytes.toString(CellUtil.cloneValue(cell))
                    map += key -> value
                }
                //把map转成json  json4s(json4scala)
                implicit val df = org.json4s.DefaultFormats
                Serialization.write(map)
            }
        })

        //{"age":"18","sex":"male","rowKey":"1001"}
        //{"name":"Janna","age":"23","sex":"female","rowKey":"1002"}
        resultRDD.collect.foreach(println)

        sc.stop()


    }

}
