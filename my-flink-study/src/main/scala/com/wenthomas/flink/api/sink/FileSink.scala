package com.wenthomas.flink.api.sink

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
/**
 * @author Verno
 * @create 2020-04-18 10:19 
 */
/**
 * Sink：将数据流输出到文件中
 */
object FileSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
                    }
                })

        //将数据流输出到文件中
        dataStream.addSink(
            StreamingFileSink.forRowFormat(
                new Path("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\output.txt"),
                new SimpleStringEncoder[String]("UTF-8")
            ).build()
        )

        dataStream.print("dataStream")

        env.execute("File Sink Demo")
    }

}
