package com.wenthomas.flink.tableapi.udf



import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @author Verno
 * @create 2020-04-25 11:51 
 */
/**
 * 自定义表函数：
 */
object TableFunction {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor_table.txt")
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(",")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
                    override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
                })

        val settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
        //--------------------------------------------------------------------------------------------------------------
        //1，创建一个UDF的实例
        val split = new MySplit("_")

        //2，UDF使用
        //UDF使用方式一：调用Table API
        //注意：TableFunction使用的时候，需要调用joinLateral方法
        val table1 = sensorTable
                .joinLateral(split('id) as('word, 'length))
                .select('id, 'ts, 'word, 'length)
        table1.toAppendStream[Row].print("table1")

        //UDF使用方式二：SQL实现UDF的调用
        tableEnv.createTemporaryView("sensor_table", sensorTable)
        tableEnv.registerFunction("split", split)
        val table2 = tableEnv.sqlQuery(
            s"""
                 |select id, ts, word, length
                 |from
                 |sensor_table, lateral table(split(id)) as splitid(word, length)
                 |""".stripMargin
        )
        table2.toAppendStream[Row].print("table2")

        env.execute("Table UDF TableFunction Demo")
    }
}

/**
 * 自定义TableFunction：继承TableFunction
 * 功能：对一个String， 输出用某个分隔符切割后的（word, wordLength）
 * 可以一进一出或一进多出
 * @param separator
 */
class MySplit(separator: String) extends TableFunction[(String, Int)] {
    //必须实现一个eval方法，它的参数是当前传入的字段
    def eval(str: String) = {
        str.split(separator).foreach(
            word => collect((word, word.length))
        )
    }
}