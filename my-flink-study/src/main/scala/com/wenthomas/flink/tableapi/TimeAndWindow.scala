package com.wenthomas.flink.tableapi


import java.sql.Timestamp

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
/**
 * @author Verno
 * @create 2020-04-24 17:00 
 */
object TimeAndWindow {
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

        //将DataStream转换成Table
        //--------------------------------------------------------------------------------------------------------------
        //1.1,定义处理时间（.proctime）：由于原数据是没有这个字段的，所以只能作为一个附加的逻辑字段来表示处理时间
        val sensorTableProcessTime = tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'pt.proctime)

        sensorTableProcessTime.printSchema()
//        sensorTableProcessTime.toAppendStream[Row].print("process_time_table")
        //--------------------------------------------------------------------------------------------------------------
        //1.2,定义事件时间（.rowtime）：必须在原数据中指定时间语义作为事件时间
        //注意：使用事件时间的前提条件：1，env中指定事件时间语义； 2，assignTimestampsAndWatermarks定义WM及时间语义配置
        val sensorTableEventTime = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

        sensorTableEventTime.printSchema()
//        sensorTableEventTime.toAppendStream[Row].print("event_time_table")



        //-------------------------------------------窗口操作------------------------------------------------------------
        //2.1, Group 窗口，开启一个10s滚动窗口，统计每个传感器温度的数量
        val groupResultTable = sensorTableEventTime
                //开创
                .window(Tumble over 10.seconds on 'ts as 'tw)
                //分组
                .groupBy('id, 'tw)
                .select('id, 'id.count, 'tw.end)
        groupResultTable.toRetractStream[(String, Long, Timestamp)].print("group result")


        //2.2,Group 窗口SQL实现
        val groupResultTableSQL = tableEnv.sqlQuery(
            s"""
                 |select id, count(id), tumble_end(ts, interval '10' second)
                 |from ${sensorTableEventTime}
                 |group by id,tumble(ts, interval '10' second)
                 |""".stripMargin
        )
        groupResultTableSQL.toRetractStream[(String, Long, Timestamp)].print("groupResultTableSQL")

        //2.3, Over窗口，对每个传感器统计每一行数据与前两行数据的平均温度
        val overResultTable = sensorTableEventTime.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
                .select('id, 'ts, 'id.count over 'w, 'temperature.avg over 'w)
        overResultTable.toRetractStream[(String, Timestamp, Long, Double)].print("overResultTable")

        //2.4, Over 窗口 SQL实现
        val overResultTableSQL = tableEnv.sqlQuery(
            s"""
                 |select id, ts,
                 |  count(id) over w,
                 |  avg(temperature) over w
                 |from ${sensorTableEventTime}
                 |window w as (
                 |  partition by id
                 |  order by ts
                 |  rows between 2 preceding and current row
                 |)
                 |""".stripMargin
        )
        overResultTableSQL.toRetractStream[(String, Timestamp, Long, Double)].print("overResultTableSQL")

        env.execute("Time And Window Demo")
    }

}
