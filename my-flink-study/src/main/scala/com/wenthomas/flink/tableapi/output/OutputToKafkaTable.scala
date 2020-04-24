package com.wenthomas.flink.tableapi.output


import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
/**
 * @author Verno
 * @create 2020-04-24 15:47 
 */
/**
 * Table输出至kafka
 */
object OutputToKafkaTable {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        //1, 定义一个kafka输入表
        tableEnv.connect(new Kafka().version("0.11")
                .topic("sensor_table")
                .property("zookeeper.connect", "mydata01:2181")
                .property("bootstrap.servers", "mydata01:9092"))
                .withFormat(new Csv)
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("kafka_input_table")

        //2, 对Table进行查询转换得到结果表
        val resultSQLTable = tableEnv.sqlQuery(
            s"""
                 |select id, temperature
                 |from kafka_input_table
                 |where id = 'sensor_11'
                 |""".stripMargin)
        resultSQLTable.toAppendStream[(String, Double)].print("resultSQLTable")

        //3, 定义一个连接到kafka的输出表
        tableEnv.connect(
            new Kafka()
                    .version("0.11")
                    .topic("sensor_table_sink")
                    .property("zookeeper.connect", "mydata01:2181")
                    .property("bootstrap.servers", "mydata01:9092"))
                .withFormat( new Csv() )
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("kafka_output_table")

        //将结果表输出
        resultSQLTable.insertInto("kafka_output_table")

        env.execute("Output Table To Kafka Demo")
    }

}
