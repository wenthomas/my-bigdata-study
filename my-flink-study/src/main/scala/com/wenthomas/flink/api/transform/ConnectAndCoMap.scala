package com.wenthomas.flink.api.transform

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
 * @author Verno
 * @create 2020-04-17 16:16 
 */
/**
 * 合流操作：Connect 和 CoMap
 * 注意：
 * * 1． Union之前两个流的类型必须是一样，Connect可以不一样，在之后的coMap中再去调整成为一样的。
 * * 2. Connect只能操作两个流，Union可以操作多个。
 */
object ConnectAndCoMap {
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
                .keyBy(0)

        //分流操作：以30℃为边界分流
        val splitStream = dataStream.split(data => {
            if (data.temperature > 30) Seq("high") else Seq("low")
        })
        val highStream = splitStream.select("high")
        val lowStream = splitStream.select("low")
        val allStream = splitStream.select("high", "low")

        //合流
        val warningStream = highStream.map(data => {
            (data.id, data.temperature)
        })
        //1,connect操作
        val connectedStream = warningStream.connect(lowStream)
        //coMap操作
        val resultStream = connectedStream.map(
            warningData => (warningData._1, warningData._2, "high temperature warning"),
            lowData => (lowData.id, "normal")
        )

        resultStream.print()

        env.execute("Connect & CoMap Stream Demo")
    }
}
