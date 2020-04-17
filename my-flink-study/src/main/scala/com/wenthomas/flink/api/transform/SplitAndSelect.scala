package com.wenthomas.flink.api.transform

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @author Verno
 * @create 2020-04-17 16:07 
 */
/**
 * 分流操作：Split & Select
 */
object SplitAndSelect {
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

        //1,split分流操作
        val splitStream = dataStream.split(data => {
            if (data.temperature > 30) Seq("high") else Seq("low")
        })

        //2,select选取分支
        val highStream = splitStream.select("high")
        val lowStream = splitStream.select("low")
        val allStream = splitStream.select("high", "low")


        highStream.print("highStream")
        lowStream.print("lowStream")
        allStream.print("allStream")

        env.execute("Split & Select Stream Demo")
    }

}
