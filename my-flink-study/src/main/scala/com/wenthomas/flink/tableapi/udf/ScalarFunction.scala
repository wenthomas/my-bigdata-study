package com.wenthomas.flink.tableapi.udf


import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
/**
 * @author Verno
 * @create 2020-04-25 11:36 
 */
/**
 * 自定义标量函数
 */
object ScalarFunction {
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
        val hashCode = new HashCode(10)
        //2，UDF使用
        //UDF使用方式一：调用Table API
        val table1 = sensorTable.select('id, 'ts, hashCode('id))
        table1.toAppendStream[Row].print("table1")


        //UDF使用方式二：SQL实现UDF的调用
        tableEnv.createTemporaryView("sensor_table", sensorTable)
        tableEnv.registerFunction("hashcode", hashCode)
        val table2 = tableEnv.sqlQuery("select id, ts, hashcode(id) from sensor_table")
        table2.toAppendStream[Row].print("table2")

        env.execute("Table UDF ScalarFunction Demo")
    }

}

/**
 * 自定义标量函数：继承ScalarFunction
 * 一进一出
 * @param factor
 */
class HashCode(factor: Int) extends ScalarFunction {
    //必须实现一个eval方法，它的参数是当前传入的字段，它的输出是一个Int类型的hash值
    def eval(str: String): Int = {
        str.hashCode * factor
    }
}