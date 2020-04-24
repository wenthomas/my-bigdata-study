package com.wenthomas.flink.tableapi
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}
import org.apache.flink.table.types.DataType
/**
 * @author Verno
 * @create 2020-04-24 11:07 
 */
/**
 *
 */
object TableApi {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //--------------------------------------------------------------------------------------------------------------
        //1, 创建表环境：不通过DataStream直接创建tableEnv
        //1.1, 创建老版本的流查询环境
        val settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        //1.2, 创建老版本的批查询环境
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val batchTableEnv = BatchTableEnvironment.create(batchEnv)

        //1.3, 创建blink版本的流查询环境
        val blinkSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build()
        val blinkTableEnv = StreamTableEnvironment.create(env, blinkSettings)

        //1.4, 创建blink版本的批查询环境
        val blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build()
        val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)


        //--------------------------------------------------------------------------------------------------------------
        //2, 从外部系统读取数据，在环境中注册表
        //2.1, 连接到外部系统（Csv）
        val filePath = "E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor_table.txt"
        tableEnv.connect(new FileSystem().path(filePath))
                //定义读取数据之后的格式化方法
                .withFormat(new Csv())
                //定义表结构
                .withSchema(new Schema().field("id", DataTypes.STRING())
                                        .field("timestamp", DataTypes.BIGINT())
                                        .field("temperature", DataTypes.DOUBLE()))
                //注册一张表
                .createTemporaryTable("inputTable")

        //2.2, 连接到Kafka
        tableEnv.connect(new Kafka().version("0.11")
                                    .topic("sensor_table")
                                    .property("zookeeper.connect", "mydata01:2181")
                                    .property("bootstrap.servers", "mydata01:9092"))
                .withFormat(new Csv)
                .withSchema(new Schema().field("id", DataTypes.STRING())
                                        .field("timestamp", DataTypes.BIGINT())
                                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable_kafka")


        //转换成流打印输出
        val sensorTable = tableEnv.from("inputTable")
        sensorTable.toAppendStream[(String, Long, Double)].print("sensorTable")

        val sensorTableFromKafka = tableEnv.from("inputTable_kafka")
        sensorTableFromKafka.toAppendStream[(String, Long, Double)].print("sensorTableFromKafka")

        //--------------------------------------------------------------------------------------------------------------
        //3, 表的查询
        //3.1, 简单查询，过滤投影
        val inputTable = tableEnv.from("inputTable")
        val resultTable = inputTable.select('id, 'temperature)
                .filter('id === "sensor_1")
        resultTable.toAppendStream[(String, Double)].print("resultTable")

        //3.2, SQL简单查询
        val resultSQLTable = tableEnv.sqlQuery(
            s"""
                 |select id, temperature
                 |from inputTable
                 |where id = 'sensor_1'
                 |""".stripMargin)
        resultSQLTable.toAppendStream[(String, Double)].print("resultSQLTable")


        //3.3, 简单聚合，统计每个传感器温度个数
        val aggResultTable = sensorTable.groupBy('id)
                .select('id, 'id.count as 'count)
        aggResultTable.toRetractStream[(String, Long)].print("aggResultTable")

        //3.4, SQL实现简单聚合
        val aggSQLResultTable = tableEnv.sqlQuery(
            """
                 |select id, count(id) as cnt from inputTable group by id
                 |""".stripMargin)
        aggSQLResultTable.toRetractStream[(String, Long)].print("aggSQLResultTable")


        env.execute("Table Api Demo")
    }
}
