package com.wenthomas.flink.tableapi
import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
/**
 * @author Verno
 * @create 2020-04-24 9:44 
 */
/**
 * Table 基本编程
 */
object TableApiDemo {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //--------------------------------------------------------------------------------------------------------------
        //0, 读取数据创建DataStream
        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        //1, 创建表执行环境
        val tableEnv = StreamTableEnvironment.create(env)

        //2, 基于数据流，转换成一张表，然后进行操作
        val dataTable = tableEnv.fromDataStream(dataStream)


        //3, 使用table api进行转换操作
        //3.1，调用table api，得到转换结果
        val resultTableByApi = dataTable.select("id, temperature")
                .filter("id == 'sensor_1'")

        //3.2，或者直接使用Sql语句得到转换结果
        val resultTableBySql = tableEnv.sqlQuery(
            s"""
                 |select id, temperature from
                 |${dataTable}
                 |where id = 'sensor_11'
                 |""".stripMargin)


        //4, 将table转回dataStream，打印输出
        //注意：需要引入隐式转换
        import org.apache.flink.table.api.scala._

        val resultStreamByApi = resultTableByApi.toAppendStream[(String, Double)]
        resultStreamByApi.print("resultStreamByApi")

        val resultStreamBySql = resultTableBySql.toAppendStream[(String, Double)]
        resultStreamBySql.print("resultStreamBySql")

        //打印表结构
        resultTableByApi.printSchema()

        resultTableBySql.printSchema()




        env.execute("Table API Simple Demo")
    }

}
