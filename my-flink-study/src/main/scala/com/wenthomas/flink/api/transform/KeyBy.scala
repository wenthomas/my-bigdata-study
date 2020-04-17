package com.wenthomas.flink.api.transform

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * @author Verno
 * @create 2020-04-17 15:25 
 */
object KeyBy {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val dataStream = inputStream.map({
            data => {
                val dataArray = data.split(", ")
                SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
            }
        })

        //1，可由数组元素的索引来分组
        val result1 = dataStream.keyBy(0)
        //2，也可由数据的字段名来分组
        val result2 = dataStream.keyBy("id")
        //3，也可由数据对象的字段来分组
        val result3 = dataStream.keyBy(data => data.id)
        //4，也可由自定义选择器来自定义分组
        val result4 = dataStream.keyBy(new MyIDSeletor())

        result1.print("result1")
        result2.print("result2")
        result3.print("result3")
        result4.print("result4")

        env.execute("KeyBy Demo")
    }

}

/**
 * 自定义key选择器：继承extends KeySelector
 */
class MyIDSeletor() extends KeySelector[SensorReading, String] {
    override def getKey(in: SensorReading): String = {
        in.id
    }
}
