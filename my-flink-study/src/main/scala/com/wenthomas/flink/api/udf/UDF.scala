package com.wenthomas.flink.api.udf

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
/**
 * @author Verno
 * @create 2020-04-17 22:46 
 */
/**
 * 自定义UDF函数
 */
object UDF {
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

        //--------------------------------------------------------------------------------------------------------------
        //1,自定义MapFuncion
        val mapStream = dataStream.map(new MyMapper)

        //--------------------------------------------------------------------------------------------------------------
        //2,自定义ReduceFunction
        val reduceStream = dataStream.reduce(new MyReducer)

        //--------------------------------------------------------------------------------------------------------------
        //3,自定义RichFunction
        val richFunctionStream = dataStream.map(new MyRichMapper)


        mapStream.print("mapStream")
        reduceStream.print("reduceStream")
        richFunctionStream.print("richFunctionStream")

        env.execute("UDF Stream Demo")
    }
}

/**
 * 自定义MapFuncion
 */
class MyMapper extends MapFunction[SensorReading, (String, Double)] {
    override def map(value: SensorReading): (String, Double) = {
        (value.id, value.temperature)
    }
}

/**
 * 自定义ReduceFunction
 */
class MyReducer extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature))
    }
}

/**
 * 自定义RichFunction
 * 加强版UDF函数
 */
class MyRichMapper extends RichMapFunction[SensorReading, (String, Int)] {
    override def open(parameters: Configuration): Unit = {}

    override def map(value: SensorReading): (String, Int) = {
        (value.id, value.timestamp.toInt)
    }

    override def close(): Unit = {}
}

