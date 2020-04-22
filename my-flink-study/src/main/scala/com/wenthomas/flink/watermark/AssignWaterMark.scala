package com.wenthomas.flink.watermark

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, TimestampAssigner}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @author Verno
 * @create 2020-04-20 11:22 
 */
object AssignWaterMark {
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

        //----------------------------------------Watermark的引入--------------------------------------------------------
        //1, 生成自增WM：对于排好序的数据，不需要延迟触发，可以只指定时间戳就行了
        //注意：单位是毫秒
        val ascendingWMStream = dataStream.assignAscendingTimestamps(_.timestamp * 1000)


        //--------------------------------------------------------------------------------------------------------------
        //2，标准WM API调用：调用 assignTimestampAndWatermarks 方法，传入一个 BoundedOutOfOrdernessTimestampExtractor，指定 watermark
        val standardWMStream = dataStream.assignTimestampsAndWatermarks(
            //参数为等待延迟的时间
            new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
                //提取时间戳：抽取数据中关于时间的字段
                override def extractTimestamp(element: SensorReading): Long = {
                    element.timestamp * 1000
                }
            }
        )

        //--------------------------------------------------------------------------------------------------------------
        //3，自定义TimestampAssigner：Flink 暴露了 TimestampAssigner 接口供我们实现，使我们可以自定义如何从事件数据中抽取时间戳和生成watermark
        //注意：MyAssigner 可以有两种类型，都继承自 TimestampAssigner

        //3.1，周期性的生成 watermark：系统会周期性的将 watermark 插入到流中，默认周期为200ms
        val customWM1Stream = dataStream.assignTimestampsAndWatermarks(new MyPeriodicAssigner(1000L))

        //3.2，断点式生成watermark：没有时间周期规律，可打断的生成 watermark
        val customWM2Stream = dataStream.assignTimestampsAndWatermarks(new PunctuatedAssigner)


        env.execute("Watermark Demo")
    }

}

/**
 * 自定义周期式WM生成器：周期性的生成 watermark，继承自AssignerWithPeriodicWatermarks
 *
 * @param lateness  等待延时
 */
class MyPeriodicAssigner(lateness: Long) extends AssignerWithPeriodicWatermarks[SensorReading] {
    //需要两个关键参数：延迟时间，以及当前所有数据中最大时间戳
    var maxTs: Long = Long.MinValue + lateness

    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTs - lateness)
    }

    //抽取时间戳
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        maxTs = maxTs.max(element.timestamp * 1000L)
        element.timestamp * 1000L
    }
}

/**
 * 自定义断点式WM生成器：没有时间周期规律，可打断的生成 watermark，继承自AssignerWithPunctuatedWatermarks
 */
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {

    val lateness = 1000L

    //自定义WM触发逻辑
    override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
        if (lastElement.id == "sensor_1") {
            new Watermark(extractedTimestamp - lateness)
        } else null
    }

    //提取时间戳
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        element.timestamp * 1000L
    }
}


