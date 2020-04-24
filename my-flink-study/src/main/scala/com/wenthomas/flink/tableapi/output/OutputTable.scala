package com.wenthomas.flink.tableapi.output

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
 * @author Verno
 * @create 2020-04-24 14:42
 */
object OutputTable {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor_table.txt")
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(",")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        val settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        //1，将dataStream转换成table：指定字段名及位置
        val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp)

        sensorTable.printSchema()
        sensorTable.toAppendStream[(String, Double, Long)].print("sensorTable")

        //2，对Table进行转换得到结果表
        val resultSQLTable = tableEnv.sqlQuery(
            s"""
                 |select id, temp
                 |from ${sensorTable}
                 |where id = 'sensor_11'
                 |""".stripMargin)
        resultSQLTable.toAppendStream[(String, Double)].print("resultSQLTable")

        //3，定义一张输出表：这就是要写入数据的TableSink，将结果输出到文件中
        //注意：withSchema配置字段要与输入到该输出表的结果表字段匹配上
        tableEnv.connect(new FileSystem().path("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\out.txt"))
                        .withFormat(new Csv)
                        .withSchema(
                            new Schema().field("id", DataTypes.STRING())
                                    .field("temp", DataTypes.DOUBLE()))
                        .createTemporaryTable("outputTable")

        //4，将结果表写入table sink
        //注意：insertInto只适用于不断插入新数据的结果表，不适用于有更新操作的结果表（比如聚合查询）
        resultSQLTable.insertInto("outputTable")


        env.execute("Output Table Demo")
    }

}
