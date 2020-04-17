package com.wenthomas.flink.api.transform

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * @author Verno
 * @create 2020-04-17 15:25 
 */
object ReduceDemo {
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

        //使用reduce聚合
        val result1 = dataStream.reduce((curTemp, newTemp) => {
            SensorReading(curTemp.id, curTemp.timestamp.max(newTemp.timestamp), curTemp.temperature.max(newTemp.temperature))
        })

        //自定义reduce函数
        val result2 = dataStream.reduce(new MyReduce())

//        result1.print("result1")
        result2.print("result2")


        env.execute("KeyBy Demo")
    }

}

/**
 * 自定义聚合函数：继承ReduceFunction
 */
class MyReduce() extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature))
    }
}
