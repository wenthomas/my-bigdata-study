package com.wenthomas.flink.api.transform

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @author Verno
 * @create 2020-04-17 16:28 
 */
/**
 * 合流：Union操作
 * 注意：
 * 1． Union之前两个流的类型必须是一样，Connect可以不一样，在之后的coMap中再去调整成为一样的。
 * 2. Connect只能操作两个流，Union可以操作多个。
 */
object Union {
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

        //合流操作：Union
        val resultStream = highStream.union(lowStream)

        resultStream.print("resultStream")

        env.execute("Union Stream Demo")
    }
}
