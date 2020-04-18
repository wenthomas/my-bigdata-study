package com.wenthomas.flink.window

import java.lang

import com.wenthomas.flink.api.source.SensorReading
import com.wenthomas.flink.api.transform.MyReduce
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-18 14:13 
 */
/**
 * Window API
 */
object Window {
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

        //--------------------------------------------------------------------------------------------------------------
        //1，开窗
        //1.1，开启滑动时间窗口：或者 .timeWindow(Time.days(1), Time.hours(-8))
        val slidingTimeWindowStream = dataStream.keyBy("id")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))

        //1.2，开启滚动时间窗口：或者 .timeWindow(Time.seconds(15))
        val tumblingWindowStream = dataStream.keyBy("id")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))

        //1.3，开启会话窗口
        val sessionWindowStream = dataStream.keyBy("id")
                .window(EventTimeSessionWindows.withGap(Time.minutes(1)))

        //1.4，开启滚动计数窗口
        val tumblingCountWindowStream = dataStream.keyBy("id")
                .countWindow(15)

        //1.5，开启滑动计数窗口
        val slidingCountWindowStream = dataStream.keyBy("id")
                .countWindow(10, 2)

        //--------------------------------------------------------------------------------------------------------------
        //2，窗口函数
        //2.1，增量聚合函数：每条数据到来就进行计算，保持一个简单的状态，包括ReduceFunction, AggregateFunction
        val resultTumblingWindow = tumblingWindowStream.reduce(new MyReduce)

        //2.2，全窗口函数：先把窗口所有数据收集起来，等到计算的时候会遍历所有数据，例如ProcessWindowFunction
        val resultFullWindow = tumblingWindowStream.apply(new MyWindowFunction)


        //resultTumblingWindow.print("resultTumblingWindow")
        resultFullWindow.print("resultFullWindow")


        env.execute("Window API Demo")
    }
}

/**
 * 自定义全窗口函数：继承WindowFunction
 * 注意：与增量聚合函数不同的是，增量聚合函数的输入数据是一条一条，而全窗口函数是一段时间范围内的所有数据
 */
class MyWindowFunction extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {
        out.collect((window.getStart, input.size))
    }
}
