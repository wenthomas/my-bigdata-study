package com.wenthomas.flink.processfunction

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-21 10:18 
 */
/**
 * 侧输出流：用于替代split算子
 */
object SideOutput {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val inputStream = env.socketTextStream("mydata01", 7777)
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        //用ProcessFunction的侧输出流实现分流操作
        val highTempStream = dataStream.process(new SplitTempProcessor(35))

        val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-temp"))

        highTempStream.print("highTempStream")
        lowTempStream.print("lowTempStream")

        env.execute("Side Output Demo")
    }

}

/**
 * 自定义ProcessFunction，用于区分高低温的数据
 * @param threshold 高低温阈值
 */
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        //判断当前数据的温度值，如果大于阈值，则输出到主流；否则输出到侧输出流
        if (value.temperature > threshold) {
            out.collect(value)
        } else {
            //侧输出流输出数据
            ctx.output(new OutputTag[(String, Double, Long)]("low-temp"), (value.id, value.temperature, value.timestamp))
        }
    }
}
