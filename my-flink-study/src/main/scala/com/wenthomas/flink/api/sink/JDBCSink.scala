package com.wenthomas.flink.api.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
/**
 * @author Verno
 * @create 2020-04-18 11:30 
 */
/**
 * 自定义Sink：将数据流写入到Mysql
 */
object JDBCSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        //将数据流输出到mysql中
        dataStream.addSink(new MyJDBCSink())

        env.execute("JDBC Sink Demo")
    }
}

/**
 * 自定义一个RichSinkFunction
 */
class MyJDBCSink() extends RichSinkFunction[SensorReading] {
    //1,首先定义实现jdbc连接，以及预编译语句
    var conn: Connection = _
    var insertStatement: PreparedStatement = _
    var updateStatement: PreparedStatement = _

    //2,初始化：在open生命周期方法中创建一次数据库连接以及预编译语句
    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://mydata03:3306/flink", "root", "123456")
        insertStatement = conn.prepareStatement("insert into sensor_temp (sensor, temperature) values (?,?)")
        updateStatement = conn.prepareStatement("update sensor_temp set temperature = ? where sensor = ?")
    }

    //3,invoke：编写调用插入更新逻辑
    override def invoke(value: SensorReading): Unit = {
        //执行更新语句
        updateStatement.setDouble(1, value.temperature)
        updateStatement.setString(2, value.id)
        updateStatement.execute()
        //如果更新行数为0，则执行插入操作
        if (updateStatement.getUpdateCount == 0) {
            insertStatement.setString(1, value.id)
            insertStatement.setDouble(2, value.temperature)
            insertStatement.execute()
            println(value.id + " inserted successfully!")
        } else {
            println(value.id + " updated successfully!")
        }
    }

    //4,关闭连接：在close生命周期方法中执行一次关闭连接操作
    override def close(): Unit = {
        insertStatement.close()
        updateStatement.close()
        conn.close()
    }
}
